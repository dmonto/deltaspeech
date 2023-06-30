"""
***************************** NLP EXPERIMENTS. SPANISH CORPUS **************************************
TestCorpus -----------------------------------------------------------------------------------------
Loads the Spanish Corpus
 (c) Delta AI 2020 - Diego Montoliu -----------------------------------------------------------------
"""
# coding: utf8
import ssl
import urllib

from gensim import corpora

context = ssl._create_unverified_context()
LANGUAGE = 'sp'  # @param ['en', 'sp'] corpora = dict(  sp=('http://www.gutenberg.org/cache/epub/2000/pg2000.txt', 28142),    en=('https://cs.stanford.edu/people/karpathy/char-rnn/shakespeare_input.txt', 0))
corpus_url, text_start = corpora[LANGUAGE]
data = urllib.urlopen(corpus_url, context=context)
all_text = data.read().lower().decode('utf8')
-	Tst	print("Downloaded corpus data with {} characters.".format(len(all_text))) print("FIRST 1000 CHARACTERS: ") print(all_text[text_start:text_start+1000])
