"""
***************************** CASER PROJECT. POST-PROC POC ******************************************
TestSpacyLemma ===========================================================================
Using Spacy as Lemmatizer
Make sure your downloaded the model with "python -m spacy download en/es"
(c) Delta AI 2020 - Diego Montoliu =================================================================
"""


# Dependencies ---------------------------------------------------------------------------------------
import os
import pandas as pd
import spacy
# ----------------------------------------------------------------------------------------------------


# Config ---------------------------------------------------------------------------------------------
nlp = spacy.load('es_core_news_sm')     # Super slow!
directory = "."
# ----------------------------------------------------------------------------------------------------


# lemmatizer() ---------------------------------------------------------------------------------------
def lemmatizer(text):
    doc = nlp(text)     # Process pipeline
    return ' '.join([word.lemma_ for word in doc])
# --------------------------------------------------------------------------------------- lemmatizer()


# main -----------------------------------------------------------------------------------------------
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    # Select files with Trans*Human.csv format
    if filename.endswith("Human.csv") and filename.startswith("Trans"):
        fil = os.path.join(directory, filename)
        print("=====================================")
        print("Leyendo {}".format(fil))
        df = pd.read_csv(fil, sep=";")
        # Lemmatize only Human transcripts
        human = df[df["Sistema"] == "Human"]["Texto"]
        human = human.apply(lambda x: lemmatizer(x))
        print(human)
# ----------------------------------------------------------------------------------------------- main