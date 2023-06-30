"""
****************************** NLP EXPERIMENTS. CLUSTERING ******************************************
TestScikitCluster -----------------------------------------------------------------------------------
Cluster Human Transcriptions using ScikitLearn
 (c) Delta AI 2020 - Diego Montoliu -----------------------------------------------------------------
"""
import os
import string
import collections
import pandas as pd
from nltk import word_tokenize
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from pprint import pprint


# process_text() -------------------------------------------------------------------------------------
def process_text(text, stem=True):
    """ Tokenize text and stem words removing punctuation """
    text = text.translate(string.punctuation)
    tokens = word_tokenize(text)

    if stem:
        stemmer = PorterStemmer()
        tokens = [stemmer.stem(t) for t in tokens]

    return tokens
# -------------------------------------------------------------------------------------- process_text()


# cluster_texts() -------------------------------------------------------------------------------------
def cluster_texts(texts, clusters=3):
    """ Transform texts to Tf-Idf coordinates and cluster texts using K-Means """
    vectorizer = TfidfVectorizer(tokenizer=process_text,
                                 stop_words=stopwords.words('spanish'),
                                 max_df=0.5,
                                 min_df=0.1,
                                 lowercase=True)

    tfidf_model = vectorizer.fit_transform(texts)
    km_model = KMeans(n_clusters=clusters)
    km_model.fit(tfidf_model)

    clustering = collections.defaultdict(list)

    for idx, label in enumerate(km_model.labels_):
        clustering[label].append(idx)

    return clustering
# ------------------------------------------------------------------------------------- cluster_texts()


# main() ----------------------------------------------------------------------------------------------
if __name__ == "__main__":
    directory = "."
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith("Human.csv") and filename.startswith("Trans"):
            fil = os.path.join(directory, filename)
            print("=====================================")
            print("Leyendo {}".format(fil))
            dfuno = pd.read_csv(file, sep=";")
            human = dfuno[dfuno["Sistema"] == "Human"]
            articles = human["Texto"]
            clusters = cluster_texts(articles, 7)
            pprint(dict(clusters))
# ---------------------------------------------------------------------------------------------- main()
