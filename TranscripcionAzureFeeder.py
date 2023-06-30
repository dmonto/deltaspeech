# ******************************* CASER PROJECT. AZURE POC *******************************************
# TranscripcionAzureFeeder ===========================================================================
# Feeds a directory of WAV files onto the Azure Speech Recogniser
# Requirements:
#  - Hold all WAV files on the same Azure Directory (Blob Storage)
#  - Azure Speech Key
#  - Azure Storage Connection String
#  - File #jobsfilename#: CSV containing #cols# columns
# (c) Delta AI 2020 - Diego Montoliu =================================================================


# Dependencies ---------------------------------------------------------------------------------------
import pandas as pd
from datetime import datetime
import time
import requests  # AJAX interface
import swagger_client as cris_client  # Azure Cognitive Interface
from azure.storage.blob import BlobServiceClient  # Azure File Interface
from typing import List  # For strong typing
# ----------------------------------------------------------------------------------------------------


# Config ---------------------------------------------------------------------------------------------
AZURE_AUDIO_LOCATOR = "*****"  # Azure Directory
AZURE_SPEECH_KEY = '*****'  # Authentification for Cognitive
AZURE_STORAGE_KEY = '*****'  # Locator for Storage
LOCALE = "es-ES"  # Language
jobsfilename = "TransAzureJobs.csv"  # Transcription File
cols = ("Fichero", "Trabajo", "Timestamp")  # Transcription File Structure
numllamadas = 50000  # Maximum Batch
# ----------------------------------------------------------------------------------------------------


# azuretranscribeinit() --------------------------------------------------------------------------------------------
def azuretranscribeinit():
    '''
    Initialises the Azure Speech Engine
    :return: API Handler
    '''
    print("Starting transcription client...")

    # configure API key authorization: subscription_key
    configuration = cris_client.Configuration()
    configuration.api_key['Ocp-Apim-Subscription-Key'] = AZURE_SPEECH_KEY

    # create the client object and authenticate
    client = cris_client.ApiClient(configuration)

    # create an instance of the transcription api class
    transcription_api = cris_client.CustomSpeechTranscriptionsApi(api_client=client)

    # get all transcriptions for the subscription
    transcriptions: List[cris_client.Transcription] = transcription_api.get_transcriptions()

    print("Deleting all existing completed transcriptions.")

    # delete all pre-existing completed transcriptions
    # if transcriptions are still running or not started, they will not be deleted
    for transcription in transcriptions:
        try:
            transcription_api.delete_transcription(transcription.id)
        except ValueError:
            # ignore swagger error on empty response message body: https://github.com/swagger-api/swagger-core/issues/2446
            pass

    return transcription_api
# ------------------------------------------------------------------------------------------- azuretranscribeinit()


def azuretranscribelist():
    """List the objects in an Amazon S3 bucket

    :param bucket_name: string
    :return: List of bucket objects. If error, return None.
    """

    # Retrieve the list of bucket objects
    try:
        service = BlobServiceClient.from_connection_string(conn_str=AZURE_STORAGE_KEY)
        generator = service.list_containers()
        response = []
        for obj in generator:
            cnt = service.get_container_client(obj.name).list_blobs()
            for fil in cnt:
                print(fil.name)
                response.append(f"{AZURE_AUDIO_LOCATOR}/{fil.name}")
    except:
        print("Error")
        return None
    return response


def azuretranscribeasync(ptrns, az_uri):
    try:
        transcription_definition = cris_client.TranscriptionDefinition(name="Caser", description="Caser Transcription", locale=LOCALE, recordings_url=az_uri)
        data, status, headers = ptrns.create_transcription_with_http_info(transcription_definition)
    except Exception as e:
        print("Llegamos al limite")
        time.sleep(5)
        return ""

    # extract transcription location from the headers
    transcription_location: str = headers["location"]

    # get the transcription Id from the location URI
    created_transcription: str = transcription_location.split('/')[-1]

    return created_transcription


def azuretranscriberetrieve(trns):
    completed = False

    while not completed:
        done, running, not_started = 0, 0, 0

        # get all transcriptions for the user
        transcriptions: List[cris_client.Transcription] = trns.get_transcriptions()

        # for each transcription in the list we check the status
        for transcription in transcriptions:
            if transcription.status in ("Failed", "Succeeded"):
                if transcription.status == "Succeeded":
                    done += 1
                    results_uri = transcription.results_urls["channel_0"]
                    results = requests.get(results_uri)
                    print("Transcription succeeded. Results: ")
                    print(results.content.decode("utf-8"))
                else:
                    print("Transcription failed :{}.".format(transcription.status_message))
            elif transcription.status == "Running":
                running += 1
            elif transcription.status == "NotStarted":
                not_started += 1

        completed = (running + not_started == 0)
        print("Transcriptions status: "
              "completed: {}, {} running, {} not started yet".format(
            done, running, not_started))

        # wait for 5 seconds
        time.sleep(60)
    return done


# recognize speech using Microsoft Azure Speech
# Microsoft Speech API keys 32-character lowercase hexadecimal strings
trns = azuretranscribeinit()
fils = azuretranscribelist()
df = pd.read_csv(jobsfilename, sep=";")
strt = time.time()

if fils is not None:
    # List the object names
    print(f'{len(fils)} Objects in {AZURE_AUDIO_LOCATOR}')
    hechas = 0
    yapedidas = 0
    for obj in fils:
        filename = obj
        filter = df[df["Fichero"] == filename]
        if len(filter) > 0:
            yapedidas += 1
            # print(f'  {filename} ya Pedido con trabajo {filter["Trabajo"].values[0]}')
            continue
        hechas += 1
        if hechas > numllamadas: break
        if filename.endswith(".wav"):
            now = str(datetime.now())
            res = azuretranscribeasync(trns, filename)
            if len(res) > 0:
                print(f'{filename} asignado {res}')
                lin = pd.DataFrame([[filename, res, now]], columns=cols)
                df = df.append(lin)
                if (hechas % 100) == 0:
                    print(f'{hechas + yapedidas} de {len(fils)} en proceso')
                    df.to_csv(jobsfilename, sep=";", index=False)

    df.to_csv(jobsfilename, sep=";", index=False)

done = azuretranscriberetrieve(trns)
print(f"{time.time() - strt - 60} segundos para {done} llamadas")

exit(0)
