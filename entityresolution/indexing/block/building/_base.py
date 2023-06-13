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
        # Check 
        # if isinstance(df_a, CandidatePairs):
        #     df_a, df_b = df_a.df_a, df_a.df_b
        # elif isinstance(df_a, pd.DataFrame):
        #     if df_b == None:
        #         df_b = df_a
        
        # if not self._validate_unique_index(df_a):
        #     raise Exception("'df_a' does not have unique index id's for each record.")
        # if df_b is not None:
        #     if not self._validate_unique_index(df_b):
        #         raise Exception("'df_b' does not have unique index id's for each record.")
        #     candidate_pairs = self._record_linkage(df_a, df_b, *args, **kwargs)
        #     if candidate_pairs is None:
        #         raise Exception("Method '_record_lankage()' has not been implemented yet.")
        # else:
        #     candidate_pairs = self._deduplication(df_a, *args, **kwargs)
        #     if candidate_pairs is None:
        #         raise Exception("Method '_deduplication()' has not been implemented yet.")
            
        return None

