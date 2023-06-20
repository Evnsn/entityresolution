import pandas as pd

from ._base import BaseBlocking

class AttributeEquivalenceBlocking(BaseBlocking):
    def __init__(self, left_on, right_on=None, left_suffix="_l", right_suffix="_r"):
        super().__init__(left_on, right_on, left_suffix, right_suffix)

    def deduplication(self, df: pd.DataFrame):
        super().deduplication(df)
        df = df.dropna(subset=[self.left_on]).copy() # Remove NaN (allow_nan parameter?)

        df["rec"] = df.index
        df["BKV"] = df[self.left_on] # Add column to merge Blocking Key Value on. This is to perserve the orginale Blocking Key
        merged_df = pd.merge(df, df, left_on="BKV", right_on="BKV", how='inner', suffixes=(self.left_suffix, self.right_suffix))
        merged_df = merged_df[merged_df["rec_l"] < merged_df["rec_r"]]
        merged_df = merged_df.set_index(["rec_l", "rec_r"])

        merged_df[f"AE_{self.left_on}"] = merged_df.apply(lambda row: row["BKV"], axis=1) # Add block
        merged_df = merged_df.drop("BKV", axis=1) # Remove temporary BKV column

        return merged_df

    def record_linkage(self, df_a:pd.DataFrame, df_b:pd.DataFrame):
        df_a = df_a.dropna(subset=[self.left_on]).copy() # Remove NaN (allow_nan parameter?)
        df_b = df_b.dropna(subset=[self.right_on]).copy() 

        df_a["rec"] = df_a.index
        df_b["rec"] = df_b.index
        df_a["BKV"] = df_a[self.left_on] # Add column to merge Blocking Key Value on. This is to perserve the orginale Blocking Key
        df_b["BKV"] = df_b[self.right_on] 
        merged_df = pd.merge(df_a, df_b, left_on="BKV", right_on="BKV", how='inner', suffixes=(self.left_suffix, self.right_suffix))
        merged_df = merged_df.set_index(["rec_l", "rec_r"])

        merged_df[f"AE_{self.left_on}"] = merged_df.apply(lambda row: row["BKV"], axis=1) # Add block column
        merged_df = merged_df.drop("BKV", axis=1) # Remove temporary BKV column

        return merged_df
        
        
def tmp():
    # These should be moved as a functionallity provided by CandidatePairs
    if return_as == "df": # Return as a dataframe of all candidate pairs and corresponding attributes.
        return merged_df
    
    elif return_as == "list":
        return merged_df.index.tolist()

    elif return_as == "attributes":
        """
        Makes blocking rules integratable with splink.
        """
        df_left = merged_df.reset_index(level=1)

        df_left = df_left.groupby(level=0).agg({f"AE_{self.left_on}": ", ".join})
        df_left = df_left[f"AE_{self.left_on}"].str.split(", ", expand=True)
        df_left.columns = [f"AE_{self.left_on}_" + str(i+1) for i in range(df_left.shape[1])]

        df_right = merged_df.reset_index(level=0)

        df_right = df_right.groupby(level=0).agg({f"AE_{self.right_on}": ", ".join})
        df_right = df_right[f"AE_{self.right_on}"].str.split(", ", expand=True)
        df_right.columns = [f"AE_{self.right_on}_" + str(i+1) for i in range(df_right.shape[1])]

        return (df_left, df_right)
    



