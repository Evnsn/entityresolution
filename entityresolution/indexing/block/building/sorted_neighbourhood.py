from itertools import combinations

import numpy as np
import pandas as pd

from entityresolution.utils.candidate_pairs import as_dataframe

from ._base import BaseBlocking

class SortedNeighborhoodBlocking(BaseBlocking):

    def __init__(
            self, 
            left_on: str, 
            right_on: str = None, 
            window_size = 3,
            left_suffix: str = "_l", 
            right_suffix: str = "_r"
        ):
        super().__init__(left_on, right_on, left_suffix, right_suffix)
        self.window_size = window_size

    def deduplication(self, df:pd.DataFrame):
        super().deduplication(df)

        # Sort the DataFrame by the blocking key
        sorted_df = df.sort_values(self.left_on)
        length_df = len(sorted_df.index)
        candidate_pairs = []

        for i in range(length_df):
            start_index = max(0, i - self.window_size)
            end_index = min(i + self.window_size, length_df)
            rec_1 = sorted_df.index[i]
            block = sorted_df.index[start_index:end_index]
            # Generate pairs
            candidate_pairs.extend([(rec_1, rec_2) for rec_2 in block if rec_2 != rec_1])
        
        candidate_pairs = [tuple(frozenset(pair)) for pair in candidate_pairs] # Correct order of pairs "[(1,2), (2,1)] => [(1,2), (1,2)]"
        candidate_pairs = list(set(candidate_pairs)) # Remove duplicate pairs

        # Generate a dataframe to return
        merged_df = as_dataframe(candidate_pairs, df, df)

        return merged_df
    
    def record_linkage(self, df_a:pd.DataFrame, df_b:pd.DataFrame):
        super().record_linkage(df_a, df_b)

        # Sort df_b
        df_b = df_b.sort_values(self.right_on)
        df_b_values = df_b[self.right_on].values
        candidate_pairs = []

        for rec_a, row in df_a.iterrows():
            value = row[self.left_on]
            index_placement = np.searchsorted(df_b_values, value)
            start_index = max(0, index_placement - self.window_size)
            end_index = min(index_placement + self.window_size, len(df_b))
            
            # Get block
            block = df_b.index[start_index:end_index]
            
            # Generate pairs
            candidate_pairs.extend([(rec_a, rec_b) for rec_b in block])

        candidate_pairs = list(set(candidate_pairs)) # Remove duplicate pairs
                
        # Create combinations of every pair for each block, do this for all blocks
        merged_df = as_dataframe(candidate_pairs, df_a, df_b)

        return merged_df
    