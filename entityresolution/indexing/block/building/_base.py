from typing import Union

import pandas as pd

from ....utils import CandidatePairs

class BaseBlocking():
    def __init__(
        self, 
        left_on:str,
        right_on:str = None,
        left_suffix:str = "_l",
        right_suffix:str = "_r"
    ):
        self.left_on = left_on
        self.right_on = right_on
        self.left_suffix = left_suffix
        self.right_suffix = right_suffix

    def build(
        self, 
        df_a:Union[pd.DataFrame, CandidatePairs], 
        df_b:pd.DataFrame = None,
        return_as:str = "df"
    ):
        if df_b is not None:
            return self.record_linkage(df_a, df_b)# Check input type
        elif df_b is None:
            return self.deduplication(df_a)
        
    def record_linkage(self, df_a:pd.DataFrame, df_b:pd.DataFrame):
        # Check input type
        if not (isinstance(df_a, pd.DataFrame) and isinstance(df_b, pd.DataFrame)):
            raise TypeError("Both DataFrames must be a pandas DataFrame.")
        
        # Check unique index
        if not (df_a.index.is_unique and df_b.index.is_unique):
            raise ValueError("Both DataFrames must have unique indexes.")


    def deduplication(self, df:pd.DataFrame):
        # Check input type
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Both DataFrames must be a pandas DataFrame.")
        
        # Check unique index
        if not df.index.is_unique:
            raise ValueError("Both DataFrames must have unique indexes.")
        

