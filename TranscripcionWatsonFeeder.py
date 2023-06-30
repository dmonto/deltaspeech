"""
******************************* CASER PROJECT. WATSON POC *******************************************
TranscripcionWatsonFeeder ===========================================================================
Feeds a directory of WAV files onto the Watson Speech Recogniser
Requirements:
 - Hold all WAV files on the same Directory {WATSON_AUDIO_LOCATOR}
 - Watson Password in {IBM_PASSWORD}
 - Watson Service Locator in {IBM_URL}
 - Language in {LOCALE}
(c) Delta AI 2020 - Diego Montoliu =================================================================
"""

# Dependencies ---------------------------------------------------------------------------------------
import base64  # For utf encoding
import datetime
import pandas as pd
import os
import re  # Regular expression handling
import speech_recognition as sr  # Legacy Synchronous SDK
import time
from oauthlib.common import urlencode  # For url encoding
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import json
from ibm_watson import SpeechToTextV1
from ibm_watson.websocket import RecognizeCallback, AudioSource
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
# ----------------------------------------------------------------------------------------------------


# Config ---------------------------------------------------------------------------------------------
WATSON_AUDIO_LOCATOR = ".\\"  # Wav File Directory
IBM_USERNAME = "******"  # Key-Only authentication
IBM_PASSWORD = ""******"  # Key for diego@montolius.com
IBM_URL = "https://stream-fra.watsonplatform.net/speech-to-text/api"  # Service Locator
LOCALE = "es-ES"  # Language
cols = ("Fichero", "Trabajo", "Timestamp")  # Output File Structure
jobsfilename = "TransWatsonJobs.csv"  # Data File
numllamadas = 50000  # Batch Size
# ----------------------------------------------------------------------------------------------------


# recognize_ibm_async() ------------------------------------------------------------------------------
def recognize_ibm_async(self, audio_data, username, password, language="en-US", show_all=False):
    """
    Performs speech recognition on ``audio_data`` (an ``AudioData`` instance), using the IBM Speech to Text API.
    The IBM Speech to Text username and password are specified by ``username`` and ``password``, respectively. Unfortunately, these are not available without `signing up for an account <https://console.ng.bluemix.net/registration/>`__. Once logged into the Bluemix console, follow the instructions for `creating an IBM Watson service instance <https://www.ibm.com/watson/developercloud/doc/getting_started/gs-credentials.shtml>`__, where the Watson service is "Speech To Text". IBM Speech to Text usernames are strings of the form XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX, while passwords are mixed-case alphanumeric strings.
    The recognition language is determined by ``language``, an RFC5646 language tag with a dialect like ``"en-US"`` (US English) or ``"zh-CN"`` (Mandarin Chinese), defaulting to US English. The supported language values are listed under the ``model`` parameter of the `audio recognition API documentation <https://www.ibm.com/watson/developercloud/speech-to-text/api/v1/#sessionless_methods>`__, in the form ``LANGUAGE_BroadbandModel``, where ``LANGUAGE`` is the language value.
    Returns the most likely transcription if ``show_all`` is false (the default). Otherwise, returns the `raw API response <https://www.ibm.com/watson/developercloud/speech-to-text/api/v1/#sessionless_methods>`__ as a JSON dictionary.
    Raises a ``speech_recognition.UnknownValueError`` exception if the speech is unintelligible. Raises a ``speech_recognition.RequestError`` exception if the speech recognition operation failed, if the key isn't valid, or if there is no internet connection.
    """
    assert isinstance(audio_data, sr.AudioData), "Data must be audio data"
    assert isinstance(username, str), "``username`` must be a string"
    assert isinstance(password, str), "``password`` must be a string"

    flac_data = audio_data.get_flac_data(
        convert_rate=None if audio_data.sample_rate >= 16000 else 16000,  # audio samples should be at least 16 kHz
        convert_width=None if audio_data.sample_width >= 2 else 2  # audio samples should be at least 16-bit
    )
    url = "https://stream-fra.watsonplatform.net/speech-to-text/api/v1/recognize?{}".format(urlencode({
        "profanity_filter": "false",
        "model": "{}_NarrowbandModel".format(language),
        "inactivity_timeout": -1,  # don't stop recognizing when the audio stream activity stops
    }))
    request = Request(url, data=flac_data, headers={
        "Content-Type": "audio/flac",
        "X-Watson-Learning-Opt-Out": "true",  # prevent requests from being logged, for improved privacy
    })
    authorization_value = base64.standard_b64encode("{}:{}".format(username, password).encode("utf-8")).decode("utf-8")
    request.add_header("Authorization", "Basic {}".format(authorization_value))

    try:
        response = urlopen(request, timeout=self.operation_timeout)
    except HTTPError as e:
        raise sr.RequestError("recognition request failed: {}".format(e.reason))
    except URLError as e:
        raise sr.RequestError("recognition connection failed: {}".format(e.reason))
    response_text = response.read().decode("utf-8")
    result = json.loads(response_text)

    # return results
    if show_all: return result
    if "results" not in result or len(result["results"]) < 1 or "alternatives" not in result["results"][0]:
        raise sr.UnknownValueError()

    transcription = []
    for utterance in result["results"]:
        if "alternatives" not in utterance: raise sr.UnknownValueError()
        for hypothesis in utterance["alternatives"]:
            if "transcript" in hypothesis:
                transcription.append(hypothesis["transcript"])
    return "\n".join(transcription)
#  ------------------------------------------------------------------------------ recognize_ibm_async()


# watsontranscribelist() ------------------------------------------------------------------------------
def watsontranscribelist(bucket_name):
    """List the objects in a Directory

    :param bucket_name: string
    :return: List of bucket objects. If error, return None.
    """

    # Retrieve the list of bucket objects
    try:
        response = []
        for parfich in os.listdir(bucket_name):
            if re.match(".*.wav", parfich):
                response.append(parfich)
    except:
        print("OS Error")
        return None
    return response
# ------------------------------------------------------------------------------ watsontranscribelist()


# watsontranscribeasync() ------------------------------------------------------------------------------
def watsontranscribeasync(pserv, fil):
    """Initiates Transcription of a file

    :param pserv    Watson Service handler
    :param fil      File Name
    :return: Watson id for the transcription
    """
    global df

    # recognize speech using IBM Speech to Text
    try:
        print("Prueba de Watson con {}".format(fil))
        audiofil = os.path.join(WATSON_AUDIO_LOCATOR, fil)
        r = sr.Recognizer()
        with sr.AudioFile(audiofil) as source:
            audio = r.record(source)  # read the entire audio file
            flac_data = audio.get_flac_data(
                convert_rate=None if audio.sample_rate >= 16000 else 16000,  # audio samples should be at least 16 kHz
                convert_width=None if audio.sample_width >= 4 else 4  # audio samples should be at least 16-bit
            )
        lres = pserv.create_job(audio=base64.standard_b64encode(flac_data), model="{}_NarrowbandModel".format(LOCALE))
        # res = r.recognize_ibm(audio, username=IBM_USERNAME, password=IBM_PASSWORD, show_all=False, language=LOCALE)
        resid = lres.result["id"]
        print(resid)
        df = df.append(pd.DataFrame([[fil, "Watson", resid]], columns=cols))
    except sr.UnknownValueError:
        print("IBM Speech to Text could not understand audio")
    except sr.RequestError as e:
        print("Could not request results from IBM Speech to Text service; {0}".format(e))
    return resid
# ------------------------------------------------------------------------------ watsontranscribeasync()


# RecognizeCallback::MyRecognizeCallback ---------------------------------------------------------------
class MyRecognizeCallback(RecognizeCallback):
    def __init__(self):
        RecognizeCallback.__init__(self)

    def on_transcription(self, transcript):
        print(transcript)

    def on_connected(self):
        print('Connection was successful')

    def on_error(self, error):
        print('Error received: {}'.format(error))

    def on_inactivity_timeout(self, error):
        print('Inactivity timeout: {}'.format(error))

    def on_listening(self):
        print('Service is listening')

    def on_hypothesis(self, hypothesis):
        print(hypothesis)

    def on_data(self, data):
        print(json.dumps(data, indent=2))
# --------------------------------------------------------------- RecognizeCallback::MyRecognizeCallback


def watsontranscribewebsock(pserv, fil):
    # Example using threads in a non-blocking way
    # TODO: Parametros recognize
    """Initiates Transcription of a file

    :param pserv    Watson Service handler
    :param fil      File Name
    :return: Watson id for the transcription
    """
    global df

    mycallback = MyRecognizeCallback()

    # recognize speech using IBM Speech to Text
    print("Prueba de Watson con {}".format(fil))
    try:
        with open(os.path.join(WATSON_AUDIO_LOCATOR, fil),
                  'rb') as audio_file:
            audio_source = AudioSource(audio_file)
            result = service.recognize_using_websocket(
                audio=audio_source,
                content_type='audio/flac',
                recognize_callback=mycallback,
                model='es-ES_BroadbandModel',
                max_alternatives=3, inactivity_timeout=-1)
    except sr.UnknownValueError:
        print("IBM Speech to Text could not understand audio")
    except sr.RequestError as e:
        print("Could not request results from IBM Speech to Text service; {0}".format(e))
    return result


# watsontranscriberetrieve() ------------------------------------------------------------------------------
def watsontranscriberetrieve(pserv, ptrans):
    completed = False

    jbs = pserv.check_jobs()
    while not completed:
        running, not_started = 0, 0

        # for each transcription in the list we check the status
        for transcription in jbs.result['recognitions']:
            if transcription['status'] in ("failed", "completed"):
                if transcription['status'] == "completed":
                    print(f"{transcription['id']} Transcription completed. Results: ")
                    jb = pserv.check_job(transcription['id']).result
                    if jb["status"] == "failed":
                        print(jb["details"][0]["error"])
                    else:
                        print(jb.text)
                else:
                    print("Ongoing")
            elif transcription['status'] == "running":
                running += 1
            elif transcription['status'] == "notstarted":
                not_started += 1
            else:
                print(transcription["status"])

        print("Transcriptions status: "
              "completed (this transcription): {}, {} running, {} not started yet".format(
            completed, running, not_started))

        # wait for 5 seconds
        time.sleep(60)


# ------------------------------------------------------------------------------ watsontranscriberetrieve()


# recognize speech using Microsoft Azure Speech
# Microsoft Speech API keys 32-character lowercase hexadecimal strings
authenticator = IAMAuthenticator(IBM_PASSWORD)
service = SpeechToTextV1(authenticator=authenticator)
service.set_service_url(IBM_URL)
fils = watsontranscribelist(WATSON_AUDIO_LOCATOR)
df = pd.read_csv(jobsfilename, sep=";")

if fils is not None:
    # List the object names
    print(f'{len(fils)} Objects in {WATSON_AUDIO_LOCATOR}')
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
            now = str(datetime.datetime.now())
            res = watsontranscribewebsock(service, filename)
            if len(res) > 0:
                print(f'{filename} asignado {res}')
                lin = pd.DataFrame([[filename, res, now]], columns=cols)
                df = df.append(lin)
                if (hechas % 10) == 0:
                    print(f'{hechas + yapedidas} de {len(fils)} en proceso')
                    df.to_csv(jobsfilename, sep=";", index=False)
                    break

    print(f'{hechas + yapedidas} de {len(fils)} COMPLETADAS')
    df.to_csv(jobsfilename, sep=";", index=False)
    # watsontranscriberetrieve(service, df["Trabajo"])

# TODO: ibm_cloud_sdk_core.api_exception.ApiException: Error: This 8000hz audio input requires a narrow band model.  See https://<STT_API_ENDPOINT>/v1/models for a list of available models., Code: 400 , X-global-transaction-id: 2039f3c8d32a150df9153c9968b86136
"""
with open(join(dirname(__file__), '"******'),
          'rb') as audio_file:
    print(json.dumps(
        service.recognize(
            audio=audio_file,
            content_type='audio/wav',
            model='es-ES_NarrowbandModel',
            timestamps=True,
            word_confidence=True).get_result(),
        indent=2))
"""
