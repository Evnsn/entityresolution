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

    def deduplication(self, df:pd.DataFrame):
        super().deduplication(df)

        # Sort the DataFrame by the blocking key
        sorted_df = df.sort_values(self.left_on)
        result_list = []

        for i in range(len(sorted_df.index) - self.window_size + 1):
            sublist = sorted_df.index[i:i + self.window_size]
            result_list.append(sublist)

        
        merged_df = as_dataframe()

        return 
    
    