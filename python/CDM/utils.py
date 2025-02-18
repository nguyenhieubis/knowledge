import os
from datetime import datetime
import uuid
import json
import time

from notebookutils import mssparkutils
from pyspark.sql import functions as sf
import delta

import hashlib

def spark_fs_mount(storage_account, container, account_key):
    _mount_point = f"/mnt/{storage_account}/{container}"
    _source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
    _extra_configs = {"accountKey":account_key}
    if any(mount.mountPoint == _mount_point for mount in mssparkutils.fs.mounts()):
        pass
    else:
        mssparkutils.fs.mount(
            source = _source,
            mountPoint = _mount_point,
            extraConfigs = _extra_configs
        )

class CDM2Fabric(object):
    def __init__(self, spark, workspace_id, lakehouse_id, storage_account, container, tenant_id, app_id, app_key, list_tables=[], exclude_tables=[], is_full_load=False, target_table_suffix=None, num_jobs=1, job_index=0, last_timestamp_folder=None, max_retries=0, retry_delay=1):
        self.uuid = str(uuid.uuid4())
        self.spark = spark
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.storage_account = storage_account
        self.container = container
        self.mount_point = f"/mnt/{self.storage_account}/{self.container}"
        self.mount_path = mssparkutils.fs.getMountPath(self.mount_point)
        self.tenant_id = tenant_id
        self.app_id = app_id
        self.app_key = app_key
        self.target_table_suffix = target_table_suffix
        self.job_index = job_index
        self.num_jobs = num_jobs
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.metadata_cdm_folder = f"{self.storage_account}/{self.container}_{self.num_jobs}_{self.job_index}"
        self.timestamp_default = "2024-01-01T00.00.00Z"
        # save to Lakehouse
        self.timestamp_folder_path = f'Files/__metadata_of_d365_sync/{self.metadata_cdm_folder}/last_timestamp_folder.txt'
        self.timestamp_folder_log_path = f'Files/__metadata_of_d365_sync/{self.metadata_cdm_folder}/last_timestamp_folder_log.txt'
        # read azure data lake via mount point
        self.cdm_changelog_value_path = f'file://{self.mount_path}/Changelog/Synapse.log'
        self.cdm_changelog_value_log_path = f'file://{self.mount_path}/Changelog/changelog.info'
        # read azure data lake via local path
        self.cdm_changelog_value_path_local = f'{self.mount_path}/Changelog/Synapse.log'
        if last_timestamp_folder:
            self.last_timestamp_folder = last_timestamp_folder
        else:
            self.last_timestamp_folder = self.get_last_timestamp_folder_of_synapselink()
        if is_full_load:
            self.timestamp_folders = self.get_all_timestamp_folders_of_synapselink()
        else:
            self.timestamp_folders = self.get_incremental_timestamp_folders_of_synapselink(last_timestamp_folder=self.last_timestamp_folder)

        self.dataflows = self.get_incremental_load_table_info(list_tables, exclude_tables, num_jobs=self.num_jobs, job_index=self.job_index)
        # save to Lakehouse
        self.dataflows_log_folder_path = f'Files/__log_of_d365_sync/{self.metadata_cdm_folder}'

    def __get__dict__(self):
        # remove attributes
        removes = ['spark', 'dataflows_log_folder_path']
        # replace value of attributes
        replaces = ['app_key']
        # attribute is object
        objects = []
        # result = {key: value for key, value in self.__dict__.items() if not key in removes}
        result = {}
        for key, value in self.__dict__.items():
            if not key in removes:
                if key in replaces:
                    result[key] = str(uuid.uuid4())
                else:
                    result[key] = value
        for o in objects:
            if o in result.keys():
                exec(f'result[o] = self.{o}.__get__dict__()')
        return result
    
    def should_process_a_dataflow(self, table_name, num_jobs, job_index):
        md5 = int(hashlib.md5(table_name.encode('utf-8')).hexdigest(), 16)
        return (md5 % num_jobs) == job_index
    
    def should_process_dataflows(self, num_jobs, job_index):
        def h(dataflow_config):
            table_name = dataflow_config.get('source').get('table_name')
            md5 = int(hashlib.md5(table_name.encode('utf-8')).hexdigest(), 16)
            return (md5 % num_jobs) == job_index
        return h
    
    def write_dataflows_log(self):
        date_time = datetime.now()
        yearkey = date_time.date().year
        monthkey = date_time.date().year*100 + date_time.date().month
        datekey = date_time.date().year*10000 + date_time.date().month*100 + date_time.date().day
        folder_path = f'{self.dataflows_log_folder_path}/_yearkey={yearkey}/_monthkey={monthkey}/_datekey={datekey}'
        time_str = str(date_time).replace(' ','_').replace(':','-')
        self.dataflows_log_file_path = f'{folder_path}/{time_str}_{self.uuid}.json'
        mssparkutils.fs.put(self.dataflows_log_file_path, json.dumps(self.__get__dict__(), default=str, indent=2), True)

    def update_timestamp_folder(self):
        max_timestamp_folder = max(self.timestamp_folders)
        content = str(max_timestamp_folder)
        mssparkutils.fs.put(self.timestamp_folder_path, content, True)
        # write logs
        if max_timestamp_folder > self.last_timestamp_folder:
            if mssparkutils.fs.exists(self.timestamp_folder_log_path):
                content = ',' + content
            mssparkutils.fs.append(self.timestamp_folder_log_path, content, True)

    def get_last_timestamp_folder_of_synapselink(self):
        _local_file_path = f'/lakehouse/default/{self.timestamp_folder_path}'
        result = self.timestamp_default
        if mssparkutils.fs.exists(self.timestamp_folder_path):
            with open(_local_file_path, 'r') as file:
                result = file.read()
        return result

    def get_all_timestamp_folders_of_synapselink(self):
        if mssparkutils.fs.exists(self.cdm_changelog_value_path) and mssparkutils.fs.exists(self.cdm_changelog_value_log_path):
            with open(self.cdm_changelog_value_path_local, 'r') as file:
                list_folders = file.read().split(',')
            current_folder = mssparkutils.fs.head(self.cdm_changelog_value_log_path)
            result = [f for f in list_folders if f < current_folder]
        else:
            print(f"{self.cdm_changelog_value_path} OR {self.cdm_changelog_value_log_path} not found.")
            raise ValueError("Files in folder 'Changelog' does not exist.")
        return result

    def get_incremental_timestamp_folders_of_synapselink(self, last_timestamp_folder):
        list_folders = self.get_all_timestamp_folders_of_synapselink()
        result = [f for f in list_folders if f >= last_timestamp_folder]
        return result

    def get_max_last_modified(self, folder_path):
        last_modified_times = [f.modifyTime for f in mssparkutils.fs.ls(folder_path) if f.isFile]
        
        if not last_modified_times:
            return 0  # No files in the folder
        
        # Find the maximum last modified timestamp
        max_last_modified_unix = max(last_modified_times)
        # max_last_modified_datetime = datetime.utcfromtimestamp(max_last_modified_unix)
        return max_last_modified_unix

    def get_target_table_info(self, source_table):
        table_name = source_table
        if self.target_table_suffix:
            table_name = f'{table_name}_{self.target_table_suffix}'
        table_path = f'Tables/{table_name}'
        _local_table_path = f'/lakehouse/default/{table_path}'
        table_log_path = os.path.join(table_path, '__table_log')
        _local_table_log_path = os.path.join(_local_table_path, '__table_log')
        watermark_file_path = os.path.join(table_log_path, 'watermark_value.txt')
        _local_watermark_file_path = os.path.join(_local_table_log_path, 'watermark_value.txt')
        watermark_log_file_path = os.path.join(table_log_path, 'watermark_log.txt')
        _local_watermark_log_file_path = os.path.join(_local_table_log_path, 'watermark_log.txt')
        last_watermark_value = 0
        is_exists = False
        if mssparkutils.fs.exists(watermark_file_path):
            content = mssparkutils.fs.head(watermark_file_path).split(',')
            last_watermark_value = int(content[0])
            is_exists = True
        result = {
            'table_name': table_name,
            'path': table_path,
            'watermark_file_path': watermark_file_path,
            'watermark_log_file_path': watermark_log_file_path,
            'last_watermark_value': last_watermark_value,
            'is_exists': is_exists,
        }
        return result

    def update_watermark_value(self, target_table: dict, new_watermark_value):
        watermark_file_path = target_table.get('watermark_file_path')
        watermark_log_file_path = target_table.get('watermark_log_file_path')
        content = str(new_watermark_value)
        mssparkutils.fs.put(watermark_file_path, content, True)
        if mssparkutils.fs.exists(watermark_log_file_path):
            content = ',' + content
        mssparkutils.fs.append(watermark_log_file_path, content, True)

    def get_list_of_tables_from_manifest_path(self, manifest_path, local_manifest_path):
        tables = []
        if mssparkutils.fs.exists(manifest_path):
            with open(local_manifest_path, 'r') as file:
                cdm = json.load(file)
            entities = cdm.get("entities")
            # tables = [e.get("entityName") for e in entities]
            tables = [e.get("name") for e in entities]
        else:
            print(f"{manifest_path} not found.")
            raise ValueError("Files 'model.json' does not exist.")
        return tables

    def get_incremental_source_table_info(self, list_tables=[], exclude_tables=[], num_jobs=1, job_index=0):
        '''
        Convert FROM folder has structure:
        -date_format_1
            +table_name_A
            +table_name_B
        -date_format_2
            +table_name_B
        TO folder structure:
        -table_name_A
            +date_format_1
        -table_name_B
            +date_format_1
            +date_format_2
        '''
        tables = {}
        for tf in self.timestamp_folders:
            folder_path = f'file://{self.mount_path}/{tf}'
            local_folder_path = f'{self.mount_path}/{tf}'
            _manifest_path = f'{folder_path}/model.json'
            _local_manifest_path = f'{local_folder_path}/model.json'
            _cdm_tables = self.get_list_of_tables_from_manifest_path(_manifest_path, _local_manifest_path)
            _spark_manifest_path = f"{self.container}/{tf}/model.json"
            _spark_storage = f"{self.storage_account}.dfs.core.windows.net"
            # FIXED
            # sys_folders = ['Microsoft.Athena.TrickleFeedService', 'OptionsetMetadata',]
            if list_tables:
                table_folders = [f for f in mssparkutils.fs.ls(folder_path) if f.isDir and f.name in _cdm_tables and f.name not in exclude_tables and self.should_process_a_dataflow(f.name, num_jobs, job_index) and f.name in list_tables]
            else:
                table_folders = [f for f in mssparkutils.fs.ls(folder_path) if f.isDir and f.name in _cdm_tables and f.name not in exclude_tables and self.should_process_a_dataflow(f.name, num_jobs, job_index)]
            
            for f in table_folders:
                _folder_path = os.path.join(folder_path, f.name)
                _new_watermark_value = self.get_max_last_modified(_folder_path)
                table_key = f.name
                _folder = {'name': tf, 'path': folder_path, 'storage': _spark_storage, 'manifest_path': _spark_manifest_path, 'max_last_modified': _new_watermark_value}

                if table_key in tables:  # table already exists
                    tables[table_key]['source']['folders'].append(_folder)
                    tables[table_key]['source']['new_watermark_value'] = max(tables[table_key]['source']['new_watermark_value'], _new_watermark_value)
                else:  # fetch for the first time
                    _source = {'table_name': table_key, 'folders': [_folder], 'new_watermark_value': _new_watermark_value}
                    tables[table_key] = {'source': _source}

        # Convert dictionary to a list if needed
        tables_list = list(tables.values())
        return tables_list

    def get_merge_condition(self, merge_cols: list):
        result = None
        for i , col in enumerate(merge_cols):
            if i == 0:
                result=f'source.{col}=target.{col}'
            else:
                result+=f' and source.{col}=target.{col}'
        return result

    def get_incremental_load_table_info(self, list_tables=[], exclude_tables=[], num_jobs=1, job_index=0):
        # clear parameters
        if not type(list_tables) is list:
            list_tables = [list_tables]
        if not type(exclude_tables) is list:
            exclude_tables = [exclude_tables]
        # BEGIN
        # tables = self.get_incremental_source_table_info(list_tables, exclude_tables)
        # selected_tables = list(filter(self.should_process_dataflows(num_jobs, job_index), tables))
        selected_tables = self.get_incremental_source_table_info(list_tables, exclude_tables, num_jobs, job_index)

        # Add more info
        for t in selected_tables:
            source = t.get('source')
            target = self.get_target_table_info(source.get('table_name'))
            t['target'] = target
            _last_watermark_value = target.get('last_watermark_value')
            _target_is_exists = target.get('is_exists')
            t['has_data'] = source.get('new_watermark_value')>_last_watermark_value
            if _target_is_exists:  # True: There is a file watermark_value.txt in target table | False: There is no file watermark_value.txt in target table
                t['is_first_load'] = False
                # filter folders containing new files
                source['folders'] = [f for f in source.get('folders') if f.get('max_last_modified')>_last_watermark_value]
            else:
                t['is_first_load'] = True
            t['merge_cols'] = ['Id']
            t['partition_cols'] = ['PartitionId']

        return selected_tables

    def get_source_df(self, source_table: dict):
        table_name = source_table.get('table_name')
        result_df = self.spark.createDataFrame([], schema="is_empty boolean")
        folders = source_table.get('folders')
        for i, f in enumerate(folders):
            _folder_name = f.get('name')
            _storage = f.get('storage')
            _manifest_path = f.get('manifest_path')
            _df_temp = (self.spark.read.format("com.microsoft.cdm")
            .option("mode", "permissive")
            .option("tenantId", self.tenant_id)
            .option("appId", self.app_id)
            .option("appKey", self.app_key)
            .option("storage", _storage)
            .option("manifestPath", _manifest_path)
            .option("entity", table_name)
            .load()
            )
            _df_temp = _df_temp
            if i == 0:
                result_df = _df_temp
            else:
                result_df = result_df.unionByName(_df_temp, allowMissingColumns=True)
        # if len(folders)>1:  # remove duplicates if there are multiple input files
        #     result_df = result_df.orderBy(sf.col('SinkModifiedOn').desc())
        #     result_df = result_df.dropDuplicates(['Id'])#.coalesce(2)
        # in a file: have duplicate data.
        result_df = result_df.orderBy(sf.col('SinkModifiedOn').desc())
        result_df = result_df.coalesce(1)
        result_df = result_df.dropDuplicates(['Id'])#.coalesce(2)
        # # check has data
        # _start_time = datetime.now()
        # source_table['has_data'] = not not df.select(sf.lit(True).alias('have_data')).take(1)
        # print(datetime.now()-_start_time)
        # add more columns
        result_df = result_df.withColumn('PartitionId', sf.year('createdon'))
        result_df = result_df.withColumn('__source_folder', sf.lit(_folder_name))
        result_df = result_df.withColumn('__updated_at', sf.from_utc_timestamp(sf.current_timestamp(), "UTC"))
        return result_df
    
    def transform_a_dataflow(self, dataflow):
        # BEGIN
        __flag = True # True: SUCCEEDED| False: By pass, no process| ERROR: FAILED
        source_table = dataflow.get('source')
        target_table = dataflow.get('target')
        target_table_path = target_table.get('path')
        
        # write to target table
        if dataflow.get('is_first_load'):
            source_df = self.get_source_df(source_table)
            partition_cols = dataflow.get('partition_cols')
            write_df = source_df.write
            if partition_cols:
                write_df = write_df.partitionBy(partition_cols)
            write_df.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_table_path)
        elif dataflow.get('has_data'):
            source_df = self.get_source_df(source_table)
            dtb = delta.DeltaTable.forPath(self.spark, target_table_path)
            merge_condition = self.get_merge_condition(dataflow.get('merge_cols'))
            (
                dtb.alias('target')
                .merge(source_df.alias('source'), merge_condition )
                .whenMatchedUpdate(condition="source.IsDelete = true", set=
                                    {
                                        "IsDelete": "source.IsDelete",
                                        "__updated_at": "source.__updated_at"
                                    }
                )
                .whenMatchedUpdateAll(condition="source.IsDelete = false OR source.IsDelete IS NULL")
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            # pass
            __flag = False

        # write watermark_value in target table
        if __flag:
            new_watermark_value = source_table.get('new_watermark_value')
            self.update_watermark_value(target_table, new_watermark_value)
        else:
            print(f"[INFO] Table {source_table.get('table_name')} has no new data.")

        return __flag
    
    def transforms(self):
        self.job_start_time = datetime.now()
        _count_failed = 0
        _count_by_pass = 0
        retry_list = []
        for i, dataflow in enumerate(self.dataflows):
            try:
                __start_time = datetime.now()
                dataflow['status'] = 'STARTING'
                _is_success = self.transform_a_dataflow(dataflow)
                if _is_success:
                    dataflow['status'] = 'SUCCEEDED'
                else:
                    _count_by_pass += 1
                    dataflow['status'] = 'SKIPPED'
            except Exception as error:
                dataflow['status'] = 'FAILED'
                dataflow['messages'] = error
                retry_list.append((i, 0))   # 0 is number of retry
            finally:
                __end_time = datetime.now()
                dataflow['start_time'] = __start_time
                dataflow['end_time'] = __end_time
                dataflow['time_span'] = __end_time-__start_time
                

        while retry_list:
            index, attempt = retry_list.pop(0)
            dataflow = self.dataflows[index]
            if attempt < self.max_retries:
                try:
                    __start_time = datetime.now()
                    dataflow['status'] = 'STARTING'
                    _is_success = self.transform_a_dataflow(dataflow)
                    if _is_success:
                        dataflow['status'] = 'SUCCEEDED'
                    else:
                        _count_by_pass += 1
                        dataflow['status'] = 'SKIPPED'
                except Exception as error:
                    dataflow['status'] = 'FAILED'
                    dataflow['messages'] = error
                    retry_list.append((index, attempt+1))
                    time.sleep(self.retry_delay)
                finally:
                    __end_time = datetime.now()
                    dataflow['start_time'] = __start_time
                    dataflow['end_time'] = __end_time
                    dataflow['time_span'] = __end_time-__start_time
                    dataflow['retry'] = attempt+1
            else:
                _count_failed += 1
                print(f"[ERROR] Source table: {dataflow.get('source').get('table_name')} ⇨ Target table: {dataflow.get('target').get('table_name')}.")
        # end for

        # If there are no errors, save the last timestamp folder
        if _count_failed == 0:
            self.update_timestamp_folder()
        self.job_end_time = datetime.now()
        self.job_time_span = (self.job_end_time-self.job_start_time)
        print("[INFO] Total time: {}".format(self.job_time_span))
        self.job_total_dataflows = len(self.dataflows)
        self.job_total_dataflows_failed = _count_failed
        self.job_total_dataflows_succeeded = self.job_total_dataflows - self.job_total_dataflows_failed
        self.job_total_dataflows_skipped = _count_by_pass
        self.job_total_dataflows_loaded = self.job_total_dataflows_succeeded - self.job_total_dataflows_skipped
        print(f"[INFO] Total SUCCEEDED: {self.job_total_dataflows_succeeded}/{self.job_total_dataflows}    |Total FAILED: {self.job_total_dataflows_failed}/{self.job_total_dataflows}")
        print(f"[INFO] Total TRANSFER: {self.job_total_dataflows_loaded}/{self.job_total_dataflows}")
        # write log
        self.write_dataflows_log()

    # def transforms(self):
    #     self.job_start_time = datetime.now()
    #     _count_failed = 0
    #     _count_by_pass = 0
    #     for dataflow in self.dataflows:
    #         try:
    #             __start_time = datetime.now()
    #             dataflow['status'] = 'STARTING'
    #             source_table = dataflow.get('source')
    #             target_table = dataflow.get('target')
    #             target_table_path = target_table.get('path')
                
    #             # write to target table
    #             if dataflow.get('is_first_load'):
    #                 source_df = self.get_source_df(source_table)
    #                 partition_cols = dataflow.get('partition_cols')
    #                 write_df = source_df.write
    #                 if partition_cols:
    #                     write_df = write_df.partitionBy(partition_cols)
    #                 write_df.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_table_path)
    #             elif dataflow.get('has_data'):
    #                 source_df = self.get_source_df(source_table)
    #                 dtb = delta.DeltaTable.forPath(self.spark, target_table_path)
    #                 merge_condition = self.get_merge_condition(dataflow.get('merge_cols'))
    #                 (
    #                     dtb.alias('target')
    #                     .merge(source_df.alias('source'), merge_condition )
    #                     .whenMatchedUpdate(condition="source.IsDelete = true", set=
    #                                        {
    #                                            "IsDelete": "source.IsDelete",
    #                                            "__updated_at": "source.__updated_at"
    #                                        }
    #                     )
    #                     .whenMatchedUpdateAll(condition="source.IsDelete = false OR source.IsDelete IS NULL")
    #                     .whenNotMatchedInsertAll()
    #                     .execute()
    #                 )
    #             else:
    #                 # pass
    #                 _count_by_pass += 1
    #                 dataflow['status'] = 'COMPLETED'
    #                 print(f"[INFO] Table {source_table.get('table_name')} has no new data.")
    #                 continue
    #             # write watermark_value in target table
    #             new_watermark_value = source_table.get('new_watermark_value')
    #             self.update_watermark_value(target_table, new_watermark_value)
    #             dataflow['status'] = 'SUCCEEDED'
    #         except Exception as error:
    #             _count_failed += 1
    #             dataflow['status'] = 'FAILED'
    #             dataflow['messages'] = error
    #             # print(f"[ERROR] Table {source_table.get('table_name')}: {error}.")
    #             print(f"[ERROR] Source table: {source_table.get('table_name')} ⇨ Target table: {target_table.get('table_name')}.")
    #         finally:
    #             __end_time = datetime.now()
    #             dataflow['start_time'] = __start_time
    #             dataflow['end_time'] = __end_time
    #             dataflow['time_span'] = __end_time-__start_time
    #     # end for
    #     # If there are no errors, save the last timestamp folder
    #     if _count_failed == 0:
    #         self.update_timestamp_folder()
    #     self.job_end_time = datetime.now()
    #     self.job_time_span = (self.job_end_time-self.job_start_time)
    #     print("[INFO] Total time: {}".format(self.job_time_span))
    #     self.job_total_dataflows = len(self.dataflows)
    #     self.job_total_dataflows_failed = _count_failed
    #     self.job_total_dataflows_succeeded = self.job_total_dataflows - self.job_total_dataflows_failed
    #     self.job_total_dataflows_skipped = _count_by_pass
    #     self.job_total_dataflows_loaded = self.job_total_dataflows_succeeded - self.job_total_dataflows_skipped
    #     print(f"[INFO] Total SUCCEEDED: {self.job_total_dataflows_succeeded}/{self.job_total_dataflows}    |Total FAILED: {self.job_total_dataflows_failed}/{self.job_total_dataflows}")
    #     print(f"[INFO] Total TRANSFER: {self.job_total_dataflows_loaded}/{self.job_total_dataflows}")
    #     # write log
    #     self.write_dataflows_log()

    def get_dataflows_failed(self):
        result = {}
        if self.job_total_dataflows_failed > 0:
            result = self.__get__dict__()
            result['dataflows'] = [f"{df.get('source').get('table_name')}=>{df.get('target').get('table_name')}" for df in result.get('dataflows') if df.get('status')=='FAILED']
        return result
