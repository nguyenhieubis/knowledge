import numpy as np
import pandas as pd

import os, datetime

from utils import *

class PreProcessing(object):
    def __init__(self, log_name, log_dir_path):
        self.log_name = log_name
        self.log_dir_path = log_dir_path
        os.makedirs(self.log_dir_path, exist_ok=True)
        self.log = Log(get_logger(self.log_name, self.log_dir_path))
        return
    
    def generate_grid_category(self, grid_df, category_cols: list):
        for col in category_cols:
            if not pd.api.types.is_categorical_dtype(grid_df[col]):
                grid_df[col] = grid_df[col].astype('category')
                
        return grid_df
    
    def generate_grid_price(self, prices_df, group_cols: list, price_col: str):
        self.log.info('generate_grid_price')

        # Pre-compute groupby transformations and store in a dictionary
        group_stats = prices_df.groupby(group_cols)[price_col].agg(
            max='max',
            min='min',
            std='std',
            mean='mean',
            nunique='nunique'
        ).rename(columns=lambda col: f"{price_col}_{col}")
        
        # Merge group_stats back into the original dataframe
        prices_df = prices_df.merge(group_stats, on=group_cols, how='left')

        # Calculate normalized price
        prices_df[f'{price_col}_norm'] = prices_df[price_col] / prices_df[f'{price_col}_max']

        # Calculate unique values for pk_cols excluding one column at a time
        if np.issubdtype(prices_df[price_col].dtype, np.float16):
            prices_df[price_col] = prices_df[price_col].astype(np.float32)
        for col in group_cols:
            remain_cols = group_cols.copy()
            remain_cols.remove(col)
            prices_df[f'{price_col}_nunique_{"_".join(remain_cols)}'] = prices_df.groupby(remain_cols + [price_col])[col].transform('nunique')

        # Calculate price momentum
        grouped_prices = prices_df.groupby(group_cols)[price_col]
        prices_df[f'{price_col}_momentum'] = prices_df[price_col] / grouped_prices.shift(1)
        prices_df[f'{price_col}_momentum_m'] = prices_df[price_col] / prices_df.groupby(group_cols + ['month'])[price_col].transform('mean')
        prices_df[f'{price_col}_momentum_y'] = prices_df[price_col] / prices_df.groupby(group_cols + ['year'])[price_col].transform('mean')

        # Calculate cents part efficiently using vectorized operation
        prices_df[f'{price_col}_cent'], _ = np.modf(prices_df[price_col])
        prices_df[f'{price_col}_max_cent'], _ = np.modf(prices_df[f'{price_col}_max'])
        prices_df[f'{price_col}_min_cent'], _ = np.modf(prices_df[f'{price_col}_min'])

        # Memory reduction step
        prices_df = Util.reduce_mem_usage(prices_df, True)
        
        return prices_df

    
    def generate_grid_date(self, grid_df, date_col: str):
        self.log.info('generate_grid_date')

        # Convert the date column to datetime if it's not already in datetime format
        if not pd.api.types.is_datetime64_any_dtype(grid_df[date_col]):
            grid_df[date_col] = pd.to_datetime(grid_df[date_col])
        
        # Pre-compute datetime attributes
        dt_series = grid_df[date_col].dt

        # Moon phase calculation - optimized with numpy
        base_date = datetime.datetime(2001, 1, 1)
        days_since_base = (grid_df[date_col] - base_date).dt.total_seconds() / 86400  # days as float
        lunations = 0.20439731 + days_since_base * 0.03386319269
        grid_df['moon'] = (np.floor((lunations % 1 * 8) + 0.5).astype(int) & 7)

        # Generate features from pre-computed datetime attributes
        grid_df['day_of_month'] = dt_series.day
        grid_df['day_of_year'] = dt_series.dayofyear
        grid_df['day_of_week'] = dt_series.dayofweek
        grid_df['week_of_year'] = dt_series.isocalendar().week.astype(np.int8)
        grid_df['month'] = dt_series.month
        grid_df['year'] = dt_series.year
        grid_df['is_wknd'] = (dt_series.weekday >= 5).astype(int)
        grid_df['is_month_start'] = dt_series.is_month_start.astype(int)
        grid_df['is_month_end'] = dt_series.is_month_end.astype(int)

        grid_df = Util.reduce_mem_usage(grid_df, True)
        return grid_df
    
    def generate_lag_feature(self, grid_df, group_cols: list, target_col: str, lags: list):
        self.log.info('generate lag features')
        grouped_df = grid_df.groupby(group_cols)
        for lag in lags:
            grid_df[f'{target_col}_lag_{lag}'] = grouped_df[target_col].shift(lag)

        grid_df = Util.reduce_mem_usage(grid_df, True)
        return grid_df
    
    def generate_roll_feature(self, grid_df, group_cols: list, target_col: str, windows: list, lags: list):
        self.log.info('generate roll features')

        # Group by the primary keys once to avoid repeating this operation
        grouped_df = grid_df.groupby(group_cols)

        for lag in lags:
            self.log.info(f'Shifting period: {lag}')
            for window in windows:
                grid_df[f'{target_col}_roll_mean_lag_{lag}_win_{window}'] = grouped_df[target_col].transform(lambda x: x.shift(lag).rolling(window).mean())
                grid_df[f'{target_col}_roll_std_lag_{lag}_win_{window}'] = grouped_df[target_col].transform(lambda x: x.shift(lag).rolling(window).std())
                
        # Memory reduction step
        grid_df = Util.reduce_mem_usage(grid_df, True)
        
        return grid_df

    def generate_ewm_feature(self, grid_df, group_cols: list, target_col: str, alphas: list, lags: list):
        self.log.info('generate exponentially weighted moving features')

        # Group by the primary keys once to avoid repeating this operation
        grouped_df = grid_df.groupby(group_cols)

        for lag in lags:
            self.log.info(f'Shifting period:{lag}')
            for alpha in alphas:
                col_name = f'{target_col}_ewm_lag_{lag}_alpha_{str(alpha).replace(".", "")}'
                grid_df[col_name] = grouped_df[target_col].transform(lambda x: x.shift(lag).ewm(alpha=alpha).mean())

        grid_df = Util.reduce_mem_usage(grid_df, True)
        return grid_df
    
    def generate_target_encoding_feature(self, grid_df, target_col: str, enc_cols: list):
        self.log.info('generate target encoding feature')

        for col in enc_cols:
            self.log.info('encoding', col)
            col_name = '_'.join(col)
            grid_df[f'enc_{col_name}_mean'] = grid_df.groupby(col)[target_col].transform('mean')
            grid_df[f'enc_{col_name}_std'] = grid_df.groupby(col)[target_col].transform('std')
        
        grid_df = Util.reduce_mem_usage(grid_df, True)
        return grid_df