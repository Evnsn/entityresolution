from ..block.building._base import BaseBlocking
from ..block.building import AttributeEquivalenceBlocking
from ...utils.candidate_pairs import as_list

"""

Resources:
- https://www.pinecone.io/learn/locality-sensitive-hashing/

"""


import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer

class MinHashLSHBlocking:
    def __init__(self, left_on, right_on=None, num_hashes=12, qgram_length=2, b=4):
        self.left_on = left_on
        self.num_hashes = num_hashes
        self.qgram_length = qgram_length
        self.b = b
        self.blk_name = f"MH_LSH_{self.left_on}"

    def build(self, df, left_on):
        self.fit(df, left_on)
        df_lsh = self.transform(df, left_on)
        return df_lsh

    def fit(self, df, attribute):
        # Generate one-hot encoding s from Q-grams of attributes values
        self.vectorizer = self._generate_vocabulary(df, attribute)
        vocab = self.vectorizer.vocabulary_
        one_hot_enc = self.vectorizer.transform(df[attribute]).toarray() 
        df = pd.DataFrame(one_hot_enc, columns=vocab)

        # Generate hash_functions
        self.hash_functions = self._generate_hash_functions()

        return df

    def _generate_vocabulary(self, df, attribute):
        vectorizer = CountVectorizer(analyzer='char', ngram_range=(self.qgram_length, self.qgram_length), binary=True)
        vectorizer.fit(df[attribute])
        return vectorizer

    def _generate_hash_functions(self):
        vocab_length = len(self.vectorizer.vocabulary_)
        hash_functions = []
        for _ in range(self.num_hashes):
            hash_function = np.random.permutation(vocab_length) + 1
            hash_functions.append(hash_function)
        return np.asarray(hash_functions)

    def transform(self, df, attribute):
        one_hot_encodings = self.vectorizer.transform(df[attribute]).toarray()
        min_hash_encodings = self._compute_signature(one_hot_encodings)

        # Split into band
        bands = self._return_bands(min_hash_encodings)
        
        bands = [['-'.join(row) for row in band.astype(str)] for band in bands]
        df_bands = pd.DataFrame(zip(*bands), columns=[f"{self.blk_name}_{i}" for i in range(len(bands))])
        
        # Cluster
        candidate_pairs = []
        for column in df_bands.columns:
            ae = AttributeEquivalenceBlocking(column)
            df_candidate_pairs = ae.build(df_bands)
            candidate_pairs.extend(as_list(df_candidate_pairs))

        return candidate_pairs

    def _compute_signature(self, records):
        hash_encoding = records[:,:,np.newaxis] * self.hash_functions.T[np.newaxis,:,:]
        hash_encoding = hash_encoding.astype('float')
        hash_encoding[hash_encoding == 0] = 'nan'
        sorted_matrix = np.sort(hash_encoding, axis=1)
        min_hash = sorted_matrix[:,0,:].astype('int')

        return min_hash

    def _return_bands(self, matrix):
        columns_l = matrix.shape[1]
        slice = columns_l // self.b
        remainding_columns = columns_l % self.b

        bands = []
        # Slices matrix into bands
        for i in range(0, columns_l - slice + 1, slice): 
            band = matrix[:,i:i+slice]
            bands.append(band)
        # Last band is obtained from the end of the matrix incase the split 
        # does not result in an equal division 
        if remainding_columns != 0: 
            band = matrix[:,-slice:] 
            bands.append(band)

        return bands



        

    # def fit(self, df, column_name):
    #     records = df[column_name].tolist()
    #     self.vectorizer = CountVectorizer(analyzer='char', ngram_range=(self.qgram_length, self.qgram_length))
    #     self.vectorizer.fit(records)
    #     self.hash_length = len(self.vectorizer.vocabulary_)
    #     self.hash_functions = self._generate_hash_functions()

    # def transform(self, df, column_name):
    #     blocks = {}
    #     records = df[column_name].tolist()
    #     vectorized_records = self.vectorizer.transform(records).toarray()
        
    #     for record_idx, record in enumerate(vectorized_records):
    #         hash_key = self._compute_hash(record)
    #         for key in hash_key:
    #             print(key)
    #         if hash_key not in blocks:
    #             blocks[hash_key] = []
    #         blocks[hash_key].append((df, record_idx))

    #     return blocks