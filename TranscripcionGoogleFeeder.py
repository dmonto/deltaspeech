"""
******************************* CASER PROJECT. BENCHMARK *******************************************
TranscripcionGoogleFeeder ===========================================================================
Feeds a directory of WAV files onto the Google Speech Recogniser
Requirements:
 - Hold all WAV files on the same Google Directory {GOOGLE_AUDIO_LOCATOR}
 - Google Keys in file {GOOGLE_CLOUD_SPEECH_CREDENTIALS}
 - Correct Language in {LOCALE}
(c) Delta AI 2020 - Diego Montoliu =================================================================
"""

# Dependencies ---------------------------------------------------------------------------------------
import pandas as pd
from datetime import datetime
import time
from google.cloud import storage, speech
from google.cloud.speech import types
# ----------------------------------------------------------------------------------------------------


# Config ---------------------------------------------------------------------------------------------
GOOGLE_AUDIO_LOCATOR = "*****"
GOOGLE_CLOUD_SPEECH_CREDENTIALS = "*****"
LOCALE = "es-ES"
cols = ("Fichero", "Trabajo", "Timestamp")
jobsfilename = "TransGoogleJobs.csv"
numllamadas = 50000     # Batch Size
# ----------------------------------------------------------------------------------------------------


def googletranscribelist(bucket_name):
    """List the objects in an Amazon S3 bucket

    :param bucket_name: string
    :return: List of bucket objects. If error, return None.
    """

    # Retrieve the list of bucket objects
    try:
        client = storage.Client()
        BUCK = bucket_name
        bucket = client.get_bucket(BUCK)

        blobs = bucket.list_blobs()

        response = []
        for blob in blobs:
            response.append(f"gs://{BUCK}/{blob.name}")
    except:
        print("Error")
        return None
    return response


def googletranscribeasync(fil):
    global df

    try:
        print("Prueba de Google con {}".format(fil))
        client = speech.SpeechClient()

        audio = types.RecognitionAudio(uri=fil)
        config = types.RecognitionConfig(language_code=LOCALE)

        operation = client.long_running_recognize(config, audio)
    except:
        print("Google Cloud Speech could not understand audio")

    return operation


def googletranscriberetrieve(ptodas):
    completed = False

    print('Waiting for operation to complete...')
    hechas = []
    df = pd.DataFrame(columns=cols)
    while not completed:
        running = 0
        not_started = 0
        completed = True
        for op in ptodas:
            opname = op.operation.name
            if opname not in hechas:
                try:
                    response = op.result(timeout=10)
                    if response is not None:
                        hechas.append(opname)
                        df = df.append(
                            pd.DataFrame([[opname, "Google", response.results[0].alternatives[0].transcript]],
                                         columns=cols))
                except:
                    running += 1
                    completed = False
        print("Transcriptions status: completed: {}".format(len(hechas), running, not_started))

        # wait for 5 seconds
        time.sleep(5)

    return df


# recognize speech using Microsoft Azure Speech
# Microsoft Speech API keys 32-character lowercase hexadecimal strings
strt = time.time()
fils = googletranscribelist(GOOGLE_AUDIO_LOCATOR)
df = pd.read_csv(jobsfilename, sep=";")

if fils is not None:
    # List the object names
    print(f'{len(fils)} Objects in {GOOGLE_AUDIO_LOCATOR}')
    hechas = 0
    yapedidas = 0
    todas = []
    for obj in fils:
        filter = df[df["Fichero"] == obj]
        if len(filter) > 0:
            yapedidas += 1
            todas.append(filter["Trabajo"].values[0])
            print(f'  {obj} ya Pedido con trabajo {filter["Trabajo"].values[0]}')
            continue
        hechas += 1
        if hechas > numllamadas: break
        if obj.endswith(".wav"):
            now = str(datetime.now())
            resop = googletranscribeasync(obj)
            resname = resop.operation.name
            if len(resname) > 0:
                todas.append(resop)
                print(f'{obj} asignado {resname}')
                lin = pd.DataFrame([[obj, resname, now]], columns=cols)
                df = df.append(lin)
                if (hechas % 100) == 0:
                    print(f'{hechas + yapedidas} de {len(todas)} en proceso')
                    df.to_csv(jobsfilename, sep=";", index=False)

    print(f'{hechas + yapedidas} de {len(todas)} PEDIDAS')

df = googletranscriberetrieve(todas)
df.to_csv(jobsfilename, sep=";", index=False)
print(f"{time.time() - strt - 60} segundos para {len(todas)} llamadas")
exit(0)
