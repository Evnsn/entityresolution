import numpy as np
import pandas as pd

from ._base import BaseBlocking

class AttributeEquivalenceBlocking(BaseBlocking):
    def __init__(self, left_on, right_on=None, left_suffix="_l", right_suffix="_r"):
        super().__init__(left_on, right_on, left_suffix, right_suffix)
        
    
    def build(self, df_a, df_b=None, return_as="df"):
        super().build(df_a, df_b, return_as)

        if df_b is None: # Deduplication
            df_a = df_a.dropna(subset=[self.left_on])

            df_a["index"] = np.arange(0, df_a.shape[0])
            merged_df = pd.merge(df_a, df_a, left_on=self.left_on, right_on=self.right_on, how='inner', suffixes=(self.left_suffix, self.right_suffix))
            merged_df = merged_df[merged_df["index_l"] < merged_df["index_r"]]
            merged_df = merged_df.set_index(["index_l", "index_r"])
            merged_df[f"AE_{self.left_on}"] = merged_df.apply(lambda row: row[self.left_on], axis=1)

        elif df_b is not None: # Record linkage
            df_a = df_a.dropna(subset=[self.left_on]).copy()
            df_b = df_b.dropna(subset=[self.right_on]).copy()

            df_a["index"] = np.arange(0, df_a.shape[0])
            df_b["index"] = np.arange(0, df_b.shape[0])
            merged_df = pd.merge(df_a, df_b, left_on=self.left_on, right_on=self.right_on, how='inner', suffixes=(self.left_suffix, self.right_suffix))
            print(merged_df.columns)
            merged_df = merged_df.set_index(["index_l", "index_r"])
            merged_df[f"AE_{self.left_on}"] = merged_df.apply(lambda row: row[self.left_on], axis=1)
        
        
        if return_as == "df":
            return merged_df
        
        elif return_as == "list":
            return merged_df.index.tolist()

        elif return_as == "attributes":
            df_left = merged_df.reset_index(level=0)

            df_left = df_left.groupby(level=0).agg({f"AE_{self.left_on}": ", ".join})
            df_left = df_left[f"AE_{self.left_on}"].str.split(", ", expand=True)
            df_left.columns = [f"AE_{self.left_on}_" + str(i+1) for i in range(df_left.shape[1])]

            df_right = merged_df.reset_index(level=0)

            df_right = df_right.groupby(level=0).agg({f"AE_{self.right_on}": ", ".join})
            df_right = df_right[f"AE_{self.right_on}"].str.split(", ", expand=True)
            df_right.columns = [f"AE_{self.right_on}_" + str(i+1) for i in range(df_right.shape[1])]

            return (df_left, df_right)
    



