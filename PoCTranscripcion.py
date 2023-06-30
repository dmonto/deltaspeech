# GOOGLE_APPLICATION_CREDENTIALS Debe ser directorio de GoogleSvc.json
import pandas as pd
import os
import speech_recognition as sr
from os import path
import boto3
from typing import List
import logging
import requests
import time
import swagger_client as cris_client
from datetime import datetime
import json

micro = False
AUDIO_FILE = "*****"
GOOGLE_AUDIO_LOCATOR = "*****"
GOOGLE_CLOUD_SPEECH_CREDENTIALS = "*****"
AWS_AUDIO_LOCATOR = "*****"
AZURE_AUDIO_LOCATOR = "*****"
AZURE_SPEECH_KEY = "*****"
WATSON_AUDIO_LOCATOR = ".\\"
IBM_USERNAME = "*****"
IBM_PASSWORD = "*****"
os.environ['AWS_DEFAULT_REGION'] = "*****"
os.environ['AWS_SHARED_CREDENTIALS_FILE'] = "*****"
transcribe = boto3.client('transcribe', region_name='*****')
LOCALE = "es-ES"
cols=("Fichero", "Sistema", "Texto")
#logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(message)s")

r = sr.Recognizer()
if micro:
    with sr.Microphone() as source:  # use the default microphone as the audio source
        print("Talk!")
        audio = r.listen(source)  # listen for the first phrase and extract it into audio data
        print("Recorded")
else:
    with sr.AudioFile(AUDIO_FILE) as source:
        audio = r.record(source)  # read the entire audio file

def transcribe_gcs(gcs_uri):
    """Asynchronously transcribes the audio file specified by the gcs_uri."""
    from google.cloud import speech
    from google.cloud.speech import enums
    from google.cloud.speech import types
    client = speech.SpeechClient()

    audio = types.RecognitionAudio(uri=gcs_uri)
    config = types.RecognitionConfig(language_code=LOCALE)

    operation = client.long_running_recognize(config, audio)

    print('Waiting for operation to complete...')
    try:
        response = operation.result(timeout=360)
    except:
        print("Timeout en Google")
        return "Timeout"

    # Each result is for a consecutive portion of the audio. Iterate through
    # them to get the transcripts for the entire audio file.
    res = ""
    for result in response.results:
        # The first alternative is the most likely one for this portion.
        #print(u'Transcript: {}'.format(result.alternatives[0].transcript))
        #print('Confidence: {}'.format(result.alternatives[0].confidence))
        res += result.alternatives[0].transcript + ". "
    return res


def azuretranscribe(az_uri):
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

    print("Creating transcriptions.")

    # Use base models for transcription. Comment this block if you are using a custom model.
    # Note: you can specify additional transcription properties by passing a
    # dictionary in the properties parameter. See
    # https://docs.microsoft.com/azure/cognitive-services/speech-service/batch-transcription
    # for supported parameters.
    transcription_definition = cris_client.TranscriptionDefinition(
        name="Caser", description="Caser Transcription", locale=LOCALE, recordings_url=az_uri
    )

    # Uncomment this block to use custom models for transcription.
    # Model information (ADAPTED_ACOUSTIC_ID and ADAPTED_LANGUAGE_ID) must be set above.
    # if ADAPTED_ACOUSTIC_ID is None or ADAPTED_LANGUAGE_ID is None:
    #     logging.info("Custom model ids must be set to when using custom models")
    # transcription_definition = cris_client.TranscriptionDefinition(
    #     name=NAME, description=DESCRIPTION, locale=LOCALE, recordings_url=RECORDINGS_BLOB_URI,
    #     models=[cris_client.ModelIdentity(ADAPTED_ACOUSTIC_ID), cris_client.ModelIdentity(ADAPTED_LANGUAGE_ID)]
    # )

    data, status, headers = transcription_api.create_transcription_with_http_info(transcription_definition)

    # extract transcription location from the headers
    transcription_location: str = headers["location"]

    # get the transcription Id from the location URI
    created_transcription: str = transcription_location.split('/')[-1]

    print("Checking status.")

    completed = False

    while not completed:
        running, not_started = 0, 0

        # get all transcriptions for the user
        transcriptions: List[cris_client.Transcription] = transcription_api.get_transcriptions()

        # for each transcription in the list we check the status
        for transcription in transcriptions:
            if transcription.status in ("Failed", "Succeeded"):
                # we check to see if it was one of the transcriptions we created from this client
                if created_transcription != transcription.id:
                    continue

                completed = True

                if transcription.status == "Succeeded":
                    results_uri = transcription.results_urls["channel_0"]
                    results = requests.get(results_uri)
                    print("Transcription succeeded. Results: ")
                    print(results.content.decode("utf-8"))
                else:
                    logging.info("Transcription failed :{}.".format(transcription.status_message))
            elif transcription.status == "Running":
                running += 1
            elif transcription.status == "NotStarted":
                not_started += 1

        print("Transcriptions status: "
                "completed (this transcription): {}, {} running, {} not started yet".format(
                    completed, running, not_started))

        # wait for 5 seconds
        time.sleep(60)

    return results.content.decode("utf-8")


def awstranscribe(aws_uri):
    job_name = "trans" + str(time.clock())
    try:
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': aws_uri},
            MediaFormat='wav',
            LanguageCode=LOCALE
        )
    except:
        print("El fichero ya ha sido transcrito")
        return ""

    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Not ready yet...")
        time.sleep(60)

    return status['TranscriptionJob']['Transcript']['TranscriptFileUri']


def transcribeuno(fil, df):
    try:
        print("Prueba de Google con {}".format(fil))
        res = transcribe_gcs(GOOGLE_AUDIO_LOCATOR + fil)
        print(res)
        df = df.append(pd.DataFrame([[fil, "Google", res]], columns=cols))
    except sr.UnknownValueError:
        print("Google Cloud Speech could not understand audio")
    except sr.RequestError as e:
        print("Could not request results from Google Cloud Speech service; {0}".format(e))

    # recognize speech using Microsoft Azure Speech
    # Microsoft Speech API keys 32-character lowercase hexadecimal strings
    try:
        print("Prueba de Azure con {}".format(fil))
        res = azuretranscribe(AZURE_AUDIO_LOCATOR + fil)
        r = json.loads(res)["AudioFileResults"][0]["CombinedResults"][0]["Display"]
        print(r)
        df = df.append(pd.DataFrame([[fil, "Azure", r]], columns=cols))
    except sr.UnknownValueError:
        print("Microsoft Azure Speech could not understand audio")
    except sr.RequestError as e:
        print("Could not request results from Microsoft Azure Speech service; {0}".format(e))

    # recognize speech using IBM Speech to Text
    try:
        print("Prueba de Watson con {}".format(fil))
        audiofil = path.join(WATSON_AUDIO_LOCATOR, fil)
        r = sr.Recognizer()
        with sr.AudioFile(audiofil) as source:
            audio = r.record(source)  # read the entire audio file
        res = r.recognize_ibm(audio, username=IBM_USERNAME,password=IBM_PASSWORD, show_all=False, language=LOCALE)
        print(res)
        df = df.append(pd.DataFrame([[fil, "Watson", res]], columns=cols))
    except sr.UnknownValueError:
        print("IBM Speech to Text could not understand audio")
    except sr.RequestError as e:
        print("Could not request results from IBM Speech to Text service; {0}".format(e))

    try:
        print("Prueba de AWS con {}".format(fil))
        res = awstranscribe(AWS_AUDIO_LOCATOR + fil)
        print(res)
        r = requests.get(res).json()["results"]["transcripts"][0]["transcript"]
        print(r)
        df = df.append(pd.DataFrame([[fil, "AWS", r]], columns=cols))
    except sr.UnknownValueError:
        print("AWS could not understand audio")
    except sr.RequestError as e:
        print("Could not request results from AWS service; {0}".format(e))
    return df


df = pd.DataFrame([[AUDIO_FILE, "Human", ""]], columns=cols)
now = str(datetime.now())[:10]
directory = "."
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.startswith("SC_1") and filename.endswith(".wav"):
        print("=====================================")
        print("Leyendo {} en {}".format(filename, now))
        df = transcribeuno(filename, df)

df.to_csv("Trans" + now + ".csv", sep=";")