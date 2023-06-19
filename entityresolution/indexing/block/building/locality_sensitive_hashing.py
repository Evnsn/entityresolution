from ._base import BaseBlocking

"""

Resources:
- https://www.pinecone.io/learn/locality-sensitive-hashing/

"""


import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer

class MinHashLSHBlocking:
    def __init__(self, num_hashes, qgram_length=2):
        self.num_hashes = num_hashes
        self.qgram_length = qgram_length

    def build(self, df, left_on):
        self.fit(df, left_on)
        df_lsh = self.transform(df, left_on)
        return df_lsh

    def fit(self, df, attribute):
        # Generate one-hot encoding s from Q-grams of attributes values
        self.vectorizer = self._generate_vocabulary(df, attribute)
        vocab = self.vectorizer.vocabulary_
        print(len(vocab))
        one_hot_enc = self.vectorizer.transform(df[attribute]).toarray() 
        df = pd.DataFrame(one_hot_enc, columns=vocab)

        # Generate hash_functions
        self.hash_functions = self._generate_hash_functions()

        return df

    def _generate_vocabulary(self, df, attribute):
        vectorizer = CountVectorizer(analyzer='char', ngram_range=(self.qgram_length, self.qgram_length))
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


        return min_hash_encodings

    def _compute_signature(self, records):
        hash_encoding = records[:,:,np.newaxis] * self.hash_functions.T[np.newaxis,:,:]
        hash_encoding = hash_encoding.astype('float')
        hash_encoding[hash_encoding == 0] = 'nan'
        sorted_matrix = np.sort(hash_encoding, axis=1)
        min_hash = sorted_matrix[:,0,:].astype('int')

        return min_hash


        

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