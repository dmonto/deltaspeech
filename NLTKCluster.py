import string
import collections

from nltk import word_tokenize
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from pprint import pprint

def demo():
    # example from figure 14.9, page 517, Manning and Schutze

    from nltk.cluster import KMeansClusterer, euclidean_distance

    vectors = [numpy.array(f) for f in [[2, 1], [1, 3], [4, 7], [6, 7]]]
    means = [[4, 3], [5, 5]]

    clusterer = KMeansClusterer(2, euclidean_distance, initial_means=means)
    clusters = clusterer.cluster(vectors, True, trace=True)

    print("Clustered:", vectors)
    print("As:", clusters)
    print("Means:", clusterer.means())
    print()

    vectors = [numpy.array(f) for f in [[3, 3], [1, 2], [4, 2], [4, 0], [2, 3], [3, 1]]]

    # test k-means using the euclidean distance metric, 2 means and repeat
    # clustering 10 times with random seeds

    clusterer = KMeansClusterer(2, euclidean_distance, repeats=10)
    clusters = clusterer.cluster(vectors, True)
    print("Clustered:", vectors)
    print("As:", clusters)
    print("Means:", clusterer.means())
    print()

    # classify a new vector
    vector = numpy.array([3, 3])
    print("classify(%s):" % vector, end=" ")
    print(clusterer.classify(vector))
    print()

    def process_text(text, stem=True):
        """ Tokenize text and stem words removing punctuation """
        text = text.translate(None, string.punctuation)
        tokens = word_tokenize(text)

        if stem:
            stemmer = PorterStemmer()
            tokens = [stemmer.stem(t) for t in tokens]

        return tokens

    def cluster_texts(texts, clusters=3):
        """ Transform texts to Tf-Idf coordinates and cluster texts using K-Means """
        vectorizer = TfidfVectorizer(tokenizer=process_text,
                                     stop_words=stopwords.words('english'),
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

    if __name__ == "__main__":
        articles = [...]
        clusters = cluster_texts(articles, 7)
        pprint(dict(clusters))