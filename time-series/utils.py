import os, random, psutil, datetime
import logging

import numpy as np
import pandas as pd

def get_logger(log_name, log_dir_path, log_level=logging.DEBUG):
    logger = logging.getLogger(log_name)
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_file_path = os.path.join(log_dir_path, (log_name + datetime.datetime.today().strftime('_%Y_%m%d.log')))
    file_handler = logging.FileHandler(filename=log_file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.setLevel(log_level)
    return logger


class Log(object):
    def __init__(self, logger):
        self.logger = logger

    def info(self, *messages):
        return self.logger.info(Log.format_message(messages))

    def debug(self, *messages):
        return self.logger.debug(Log.format_message(messages))

    def warning(self, *messages):
        return self.logger.warning(Log.format_message(messages))

    def error(self, *messages):
        return self.logger.error(Log.format_message(messages))

    def exception(self, *messages):
        return self.logger.exception(Log.format_message(messages))

    @staticmethod
    def format_message(messages):
        if len(messages) == 1 and isinstance(messages[0], list):
            messages = tuple(messages[0])
        return '\t'.join(map(str, messages))

    # def log_evaluation(self, period=100, show_stdv=True, level=logging.INFO):
    #     def _callback(env):
    #         if period > 0 and env.evaluation_result_list and (env.iteration + 1) % period == 0:
    #             result = '\t'.join(
    #                 [lgb.callback._format_eval_result(x, show_stdv) for x in env.evaluation_result_list])
    #             self.logger.log(level, '[{}]\t{}'.format(env.iteration + 1, result))

    #     _callback.order = 10
    #     return _callback

class Util(object):
    @staticmethod
    def set_seed(seed):
        random.seed(seed)
        np.random.seed(seed)
        os.environ['PYTHONHASHSEED'] = str(seed)
        return

    @staticmethod
    def get_memory_usage():
        return np.round(psutil.Process(os.getpid()).memory_info()[0] / 2. ** 30, 2)
    
    @staticmethod
    def sizeof_fmt(num, suffix='B'):
        for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    @staticmethod    
    def reduce_mem_usage(df, verbose=False):
        start_mem = df.memory_usage().sum()
        
        # Dictionary for numeric types and their respective data types
        int_types = [np.int8, np.int16, np.int32, np.int64]
        float_types = [np.float16, np.float32, np.float64]
        
        def optimize_column(col):
            col_type = col.dtype
            
            if col_type in int_types or col_type in float_types:
                c_min = col.min()
                c_max = col.max()
                
                if np.issubdtype(col_type, np.integer):
                    for dtype in int_types:
                        dtype_min = np.iinfo(dtype).min;dtype_max = np.iinfo(dtype).max
                        if dtype_min <= c_min <= dtype_max and dtype_min <= c_max <= dtype_max:
                            return col.astype(dtype)
                elif np.issubdtype(col_type, np.floating):
                    for dtype in float_types:
                        dtype_min = np.finfo(dtype).min;dtype_max = np.finfo(dtype).max
                        if dtype_min <= c_min <= dtype_max and dtype_min <= c_max <= dtype_max:
                            return col.astype(dtype)
            
            return col

        for col in df.columns:
            df[col] = optimize_column(df[col])
        
        end_mem = df.memory_usage().sum()
        
        if verbose:
            mem_reduction = 100 * (start_mem - end_mem) / start_mem
            print(f'Mem. usage decreased to {end_mem / 1e6:.2f} MB ({mem_reduction:.1f}% reduction)')
        
        return df

    @staticmethod
    def merge_by_concat(df1, df2, merge_on):
        merged_df = pd.merge(df1, df2, on=merge_on, how='left', suffixes=('', '__y'))
        merged_df.drop([col for col in merged_df.columns if col.endswith('__y')], axis=1, inplace=True)
        return merged_df
