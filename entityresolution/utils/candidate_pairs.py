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
    