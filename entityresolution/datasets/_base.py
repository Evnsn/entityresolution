import os

import pandas as pd

def load_restaurants():
    module_path = os.path.abspath(__file__)
    data_dir = os.path.join(os.path.dirname(module_path), "data")
    path = os.path.join(data_dir, "restaurants.csv")
    return pd.read_csv(path)