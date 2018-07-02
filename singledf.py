import os
import logging

import pandas as pd
import numpy as np
from .logging import make_log_msg
from . import DataFrameInspector, FailSampler

def check_col_format(df, col, regex, df_desc=None, inspector_title='check_col_format', inspection_detail=None, logger=None, fail_sampler=None):
    """
    Check if all elements of column `col` in dataframe `df` match `regex`.
    If check is OK, log an INFO message to logger, and return True.
    If check fails, log an ERROR message to logger, write failed records to a csv in `fail_sample_dir`, and return False.
    """
    if col not in df.columns:
        msg = 'Column {} not found in dataframe'.format(col) + (' '+df_desc if df_desc else '')
        logger.warning(msg) if logger else print(msg)
    
    inspection_detail = inspection_detail or 'Column {} vs. pattern {}'.format(col, regex)
    ok = df[col].astype(str).str.match(regex)
    
    if ok.all():
        msg = make_log_msg(inspector_title, status='PASS', detail=inspection_detail, extra=df_desc)
        logger.info(msg) if logger else print(msg)
        return True
    else:
        # fail info
        nrows = sum(~ok)
        inspection_detail += ' {} violations'.format(nrows)
        msg = make_log_msg(inspector_title, status='FAIL', detail=inspection_detail, extra=df_desc)
        fail_sample_filename = make_log_msg(inspector_title, status='FAIL', detail=inspection_detail, extra=df_desc, sep=' ')
        # log error
        logger.error(msg) if logger else print(msg)
        # write fail sample
        fail_sampler.write_sample(df, ~ok, save_filename=fail_sample_filename, index=False) if fail_sampler else None
        return False

def check_missing_values(df: pd.DataFrame, cols=None, df_desc=None, inspector_title='check_missing_values', inspection_detail=None, logger=None, fail_sampler=None):
    """
    Check if dataframe `df` has missing values in columns `cols`, or check all columns if `cols` is None. 
    If no missing values found, log INFO to logger and return True.
    If missing values found, log ERROR to logger, and return False.
    """
    cols = cols if cols else df.columns.tolist()
    ok = df[cols].isnull().sum(axis=0)==0  # ok should be a pd.Series with column names as index and True/False indicating if the column is ok.
    if all(ok):
        inspection_detail = inspection_detail or 'No missing values in {}'.format(cols)
        msg = make_log_msg(inspector_title, status='PASS', detail=inspection_detail, extra=df_desc)
        logger.info(msg) if logger else print(msg)
        return True
    else:
        # fail info
        fail_cols = ok[~ok].index.tolist() # a list of cols that has missing values
        inspection_detail = inspection_detail or 'Missing values found in columns {}'.format(fail_cols)
        msg = make_log_msg(inspector_title, status='FAIL', detail=inspection_detail, extra=df_desc)
        fail_sample_filename = make_log_msg(inspector_title, status='FAIL', detail=inspection_detail, extra=df_desc, sep=' ')
        # log error msg
        logger.error(msg) if logger else None
        # make fail sample
        fail_index = df[fail_cols].isnull().any(axis=1)
        fail_sampler.write_sample(df, fail_index, save_filename=fail_sample_filename, index=False) if fail_sampler else None
        return False

def check_groupby_agg(df, by, check_col, agg_func, desired_agg_val=True, almost_equal=False, agg_func_name='infer', compare_tolerance={}, df_desc=None, inspector_title=None, inspection_detail=None, logger=None, fail_sampler=None):
    """
    df.groupby(by)[check_col].agg(agg_func) should all equal to `desired_agg_val`.
    If so, log INFO message to logger, and return True.
    If not, log ERROR message to logger, write a fail sample to `fail_sample_dir`, and return False.
    When `almost_equal` is set to True, use numpy.isclose() rather than exact comparison. `compare_tolerance` is passed to numpy.isclose() as keyword arguments. 
    """
    # Prepare info about this check
    title = inspector_title or 'Group Aggregate Check'
    agg_func_name = agg_func.__name__ if agg_func_name=='infer' and hasattr(agg_func, '__name__') else str(agg_func_name)
    inspection_detail = inspection_detail or '{col} grouped by {by} aggregated by {func} compared with {val}'.format(col=check_col, by=by, func=agg_func_name, val=desired_agg_val)
    # Group & aggregate
    gb_agg = df.groupby(by)[check_col].agg(agg_func) if check_col else df.groupby(by).agg(agg_func)
    ok = gb_agg.apply(lambda x: np.isclose(x, desired_agg_val, **compare_tolerance)) if almost_equal else gb_agg==desired_agg_val
    
    if ok.all():
        msg = make_log_msg(title, status='PASS', detail=inspection_detail, extra=df_desc)
        logger.info(msg) if logger else print(msg)
        return True
    else:
        # fail info
        n_not_ok = sum(~ok)
        inspection_detail += ' {} violations'.format(n_not_ok)
        msg = make_log_msg(title, status='FAIL', detail=inspection_detail, extra=df_desc)
        fail_sample_filename = make_log_msg(title, status='FAIL', detail=inspection_detail, extra=df_desc, sep=' ')
        # log error
        logger.error(msg) if logger else print(msg)
        # make fail sample
        fail_sampler.write_sample(gb_agg, ~ok, save_filename=fail_sample_filename) if fail_sampler else None
        return False

def check_groupby_identical(df: pd.DataFrame, by, check_col, df_desc=None, inspector_title='check_group_identical', inspection_detail=None, logger=None, fail_sampler=None):
    """
    df.groupby(by)[check_col] should be identical within each group.
    If so, log INFO message to logger, and return True.
    If not, log ERROR message to logger, write a fail sample to `fail_sample_dir`, and return False.
    """
    # Info about this check
    inspection_detail = inspection_detail or '{col} grouped by {by} is identical within group?'.format(col=check_col, by=by)
    check_groupby_agg(df=df, by=by, check_col=check_col, 
                      agg_func=lambda series: series.nunique(dropna=False)<=1,
                      desired_agg_val=True,
                      df_desc=df_desc,
                      inspector_title=inspector_title, inspection_detail=inspection_detail, 
                      logger=logger, fail_sampler=fail_sampler)

def check_no_duplicate(df, cols=None, df_desc=None, inspector_title='check_no_duplicate', inspection_detail=None, logger: logging.Logger=None, fail_sampler=None):
    """
    Cominations of values in `cols` should have no duplicates.
    If so, log INFO message to logger, and return True.
    If not, log ERROR message to logger, write a fail sample to `fail_sample_dir`, and return False.
    """
    dup = df.duplicated(subset=cols, keep=False)
    if not any(dup):
        inspection_detail = 'No duplicate in combinations of {}'.format(cols)
        msg = make_log_msg(title=inspector_title, status='PASS', detail=inspection_detail, extra=df_desc)
        logger.info(msg) if logger else print(msg)
        return True
    else:
        # fail info
        n_not_ok = sum(dup)
        inspection_detail = 'Duplicates found in cols {} {} violations'.format(cols, n_not_ok)
        msg = make_log_msg(title=inspector_title, status='FAIL', detail=inspection_detail, extra=df_desc)
        fail_sample_filename = make_log_msg(inspector_title, status='FAIL', detail=inspection_detail, extra=df_desc, sep=' ')
        # log error
        logger.error(msg) if logger else print(msg)
        # write fail sample
        fail_sampler.write_sample(df, dup, save_filename=fail_sample_filename, index=False) if fail_sampler else None


class ColumnFormatInspector(DataFrameInspector):
    def __init__(self, col, regex, title='Column Format Check', detail=None, logger=None, fail_sampler=None):
        detail = detail or 'Column {} vs. pattern {}'.format(col, regex)
        super().__init__(title, detail=detail, logger=logger, fail_sampler=fail_sampler)
        self.check_col = col
        self.regex = regex
    
    def inspect(self, df, df_desc=None):
        params = {'col': self.check_col, 'regex': self.regex, 'df_desc': df_desc, 
                  'inspector_title': self.title, 'inspection_detail': self.detail,
                  'logger': self.logger, 'fail_sampler': self.fail_sampler}
        return check_col_format(df, **params)

class GroupAggregateInspector(DataFrameInspector):
    def __init__(self, by, check_col, agg_func, desired_agg_val, almost_equal=False, agg_func_name='infer', compare_tolerance={}, title='Group Aggregate Check', detail=None, logger=None, fail_sampler=None):
        try:
            agg_func_name = agg_func.__name__
        except:
            agg_func_name = str(agg_func_name)
        detail = detail or '{col} grouped by {by} aggregated by {func} compared with {val}'.format(col=check_col, by=by, func=agg_func_name, val=desired_agg_val)
        super().__init__(title=title, detail=detail, logger=logger, fail_sampler=fail_sampler)
        self.by = by
        self.check_col = check_col
        self.agg_func = agg_func
        self.desired_agg_val = desired_agg_val
        self.almost_equal = almost_equal
        self.agg_func_name = agg_func_name
        self.compare_tolerance = compare_tolerance
    
    def inspect(self, df, df_desc=None):
        params = {'by': self.by, 
                  'check_col': self.check_col, 
                  'agg_func': self.agg_func, 
                  'desired_agg_val': self.desired_agg_val,
                  'almost_equal': self.almost_equal, 
                  'agg_func_name': self.agg_func_name, 
                  'compare_tolerance': self.compare_tolerance,
                  'inspector_title': self.title, 
                  'inspection_detail': self.detail, 
                  'df_desc': df_desc, 
                  'logger': self.logger, 
                  'fail_sampler': self.fail_sampler
                 }
        return check_groupby_agg(df, **params)

class IdenticalWithinGroupInspector(DataFrameInspector):
    def __init__(self, by, check_col, almost_equal=False, title='Identical Within Group Check', detail=None, logger=None, fail_sampler=None):
        detail = detail or '{col} grouped by {by}'.format(col=check_col, by=by)
        super().__init__(title=title, detail=detail, logger=logger, fail_sampler=fail_sampler)
        self.by = by
        self.check_col = check_col
    
    def inspect(self, df, df_desc=None):
        params = {'by': self.by, 
                  'check_col': self.check_col, 
                  'inspector_title': self.title, 
                  'inspection_detail': self.detail, 
                  'df_desc': df_desc, 
                  'logger': self.logger, 
                  'fail_sampler': self.fail_sampler
                 }
        return check_groupby_identical(df, **params)

class NoDuplicateInspector(DataFrameInspector):
    def __init__(self, cols, title='No Duplicate Check', detail=None, logger=None, fail_sampler=None):
        self.cols = cols
        super().__init__(title=title, detail=detail, logger=logger, fail_sampler=fail_sampler)
    
    def inspect(self, df, df_desc=None):
        return check_no_duplicate(df, cols=self.cols, 
                                  df_desc=df_desc,
                                  inspector_title=self.title,
                                  inspection_detail=self.detail, 
                                  logger=self.logger,
                                  fail_sampler=self.fail_sampler)

class MissingValuesInspector(DataFrameInspector):
    def __init__(self, cols=None, title='Missing Values Check', detail=None, logger=None, fail_sampler=None):
        super().__init__(title=title, detail=detail, logger=logger, fail_sampler=fail_sampler)
        self.cols = cols
    
    def inspect(self, df, df_desc=None):
        return check_missing_values(df, cols=self.cols, df_desc=df_desc, inspector_title=self.title, inspection_detail=self.detail, logger=self.logger, fail_sampler=self.fail_sampler)