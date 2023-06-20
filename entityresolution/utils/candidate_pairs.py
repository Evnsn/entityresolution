import numpy as np
import pandas as pd

class CandidatePairs():
    def __init__(self, blocks:dict, df_a:pd.DataFrame, df_b:pd.DataFrame):
        self.blocks= blocks
        self.df_a = df_a.copy()
        self.df_b = df_b.copy()

    def as_dataframe(self):
        return pd.DataFrame()
    
    def as_list(self):
        return [(1,2) for _ in range(5)]


def as_dataframe(pairs, df_a, df_b):
    # df_a_copy = df_a.copy()
    # df_b_copy = df_b.copy()
    
    # mapping = {a: b for a, b in pairs} # Create a mapping dictionary from pairs
    
    # df_a_copy['corresponding'] = df_a_copy.index.map(mapping) # Add a new column to df_a with the corresponding values from df_b
    # merged_df = pd.merge(df_a_copy, df_b_copy, left_on='corresponding', right_index=True, how='left', suffixes=("_l", "_r")) # Merge df_a_copy and df_b_copy based on the 'corresponding' column
    # merged_df.drop('corresponding', axis=1, inplace=True)# Drop the 'corresponding' column

    # return merged_df

    pairs = np.array(pairs) # dtype=int

    # Select the rows based on the pairs of indexes
    unified_df_a = df_a.loc[pairs[:, 0]]
    unified_df_a["index"] = unified_df_a.index
    unified_df_a = unified_df_a.reset_index(drop=True)
    unified_df_a = unified_df_a.add_suffix("_l")

    unified_df_b = df_b.loc[pairs[:, 1]]
    unified_df_b["index"] = unified_df_b.index
    unified_df_b = unified_df_b.reset_index(drop=True)
    unified_df_b = unified_df_b.add_suffix("_r")

    # Create the unified dataframe with the multi-index
    unified_df = pd.concat([unified_df_a, unified_df_b], axis=1)
    unified_df = unified_df
    unified_df = unified_df.set_index(["index_l", "index_r"])

    return unified_df

def as_list(df):
    return df.index.tolist()

def as_attributes(pairs, df_a, df_b, attribute = "category"):
    """
    Makes blocking rules integratable with splink.
    """
    df_a = df_a.dropna(subset=[attribute]).copy() # Remove NaN (allow_nan parameter?)
    df_b = df_b.dropna(subset=[attribute]).copy()
    
    merged_df = as_dataframe(pairs, df_a, df_b)
    merged_df[f"AE_{attribute}"] = merged_df.apply(lambda row: row[f"{attribute}_l"], axis=1) # Add block_id

    df_left = merged_df.reset_index(level=1) # Seperate left dataframe

    df_left = df_left.groupby(level=0).agg({f"AE_{attribute}": ", ".join})
    df_left = df_left[f"AE_{attribute}"].str.split(", ", expand=True)
    df_left.columns = [f"AE_{attribute}_" + str(i+1) for i in range(df_left.shape[1])]
    df_left = pd.concat([df_a, df_left], axis=1)

    df_right = merged_df.reset_index(level=0) # Seperate left dataframe

    df_right = df_right.groupby(level=0).agg({f"AE_{attribute}": ", ".join})
    df_right = df_right[f"AE_{attribute}"].str.split(", ", expand=True)
    df_right.columns = [f"AE_{attribute}_" + str(i+1) for i in range(df_right.shape[1])]
    df_right = pd.concat([df_b, df_right], axis=1)

    return (df_left, df_right)