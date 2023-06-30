import pandas as pd
import os
import requests
import time
from datetime import datetime
import boto3
import json

#bucket_name = "*****"
#region = '*****'
#AWS_AUDIO_LOCATOR = "*****"
profile = "*****"
bucket_name = "*****"
region = "*****"
key_id = '*****'
secret_key = '*****'
AWS_AUDIO_LOCATOR = f'https://{bucket_name}.s3.{region}.amazonaws.com/'
os.environ['AWS_DEFAULT_REGION'] = region
os.environ['AWS_SHARED_CREDENTIALS_FILE'] = "*****"
session = boto3.session.Session(aws_access_key_id=key_id,aws_secret_access_key=secret_key)
s3 = session.resource('s3')
boto3.setup_default_session(aws_access_key_id=key_id,aws_secret_access_key=secret_key)
transcribe = boto3.client('transcribe', region_name=region)
s3_client = boto3.client('s3')
LOCALE = "es-ES"
colsjbs=("Fichero", "Trabajo", "Timestamp")
colsdf=("Fichero", "Trabajo", "Timestamp", "Transcript")
dire="."

jobsfilename= "TransAWSJobs.csv"
jobsurl=AWS_AUDIO_LOCATOR + jobsfilename
resultsfilename= "TransAWSResults.csv"
resultsurl=AWS_AUDIO_LOCATOR + resultsfilename
porsubir = list()


def leeficheros():
    global jbs, transcritos, df

    try:
        jbs = pd.read_csv(jobsurl, sep=";", usecols=colsjbs)
    except Exception as e:
        print("No puedo abrir "+ jobsurl)
        print(e)
        exit(1)
    df = pd.read_csv(resultsurl, sep=";", usecols=colsdf, index_col=0)
    transcritos = list(df.index.values)
    return


def subes3(pporsubir):
    s3_client.upload_file(dire + resultsfilename, bucket_name, resultsfilename)
    s3.ObjectAcl(bucket_name, resultsfilename).put(ACL='public-read')
    for jsonname in pporsubir:
        localname = dire + jsonname
        s3name = "Results/" + jsonname
        s3_client.upload_file(localname, bucket_name, s3name)
        s3.ObjectAcl(bucket_name, s3name).put(ACL='public-read')
        print("{} Subido".format(s3name))
    print("{} Hechos, {} por transcribir ".format(len(transcritos), len(jbs) - len(transcritos)))
    return list()


def grabatrans(pstatus):
    global transcritos, porsubir, df

    if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
        res = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
        resjson = requests.get(res).json()
        r = resjson["results"]["transcripts"][0]["transcript"]
        print("{} Completed".format(fname))
        jsonname = '{}.json'.format(fname[:-4].replace("/","-"))
        localname = dire + jsonname
        with open(localname, 'w', encoding='utf8') as json_file:
            json.dump(resjson, json_file, ensure_ascii=False)
            porsubir.append(jsonname)

        df = df.append(pd.DataFrame([[fname, job, str(datetime.now()), r]], index=[fname], columns=colsdf))
        if len(transcritos) % 100 == 0:
            df.to_csv(dire + resultsfilename, sep=";", encoding="utf_8", index=True)
            porsubir = subes3(porsubir)
    elif status['TranscriptionJob']['TranscriptionJobStatus'] == 'FAILED':
        print("{} Failed".format(fname))
    return

def rehacer(pfich):
    wavfil = pfich[:-4].replace("-","/")+"wav"
    print("Rehacer " + wavfil)
    resultsfilename = "TransAWSResults.csv"
    resultsurl = AWS_AUDIO_LOCATOR + resultsfilename
    df = pd.read_csv(resultsurl, sep=";", usecols=colsdf, index_col=0)
    df = df.drop(wavfil)
    df.to_csv(dire + resultsfilename, sep=";", encoding="utf_8", index=True)
    s3_client.upload_file(dire + resultsfilename, bucket_name, resultsfilename)
    s3.ObjectAcl(bucket_name, resultsfilename).put(ACL='public-read')
    return

def limpiaresults():
    global df

    jsons = os.listdir(dire)
    for fil in transcritos:
        jsfil = (fil[:-4]+".json").replace("/","-")
        if jsfil not in jsons:
            rehacer(jsfil)
            transcritos.remove(fil)
    return

leeficheros()
limpiaresults()
while len(transcritos) < len(jbs):
    for ix, f in jbs.iterrows():
        fname = f["Fichero"]
        job = f["Trabajo"]
        if fname not in transcritos:
            try:
                status = transcribe.get_transcription_job(TranscriptionJobName=job)
                if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                    grabatrans(status)
                    transcritos.append(fname)
                else:
                    print("{} no esta listo...".format(fname))
                    porsubir = subes3(porsubir)
                    time.sleep(5)
                    print("Trying again...")
            except Exception as e:
                print("{} VACIO, REHACER {}".format(fname, job))
                transcritos.append(fname)
    df.to_csv(dire + resultsfilename, sep=";", encoding="utf_8", index=True)
    porsubir = subes3(porsubir)
    leeficheros()

exit(0)

