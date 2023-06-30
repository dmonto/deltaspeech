import pandas as pd
import os
import boto3
from botocore.exceptions import ClientError
import json
import requests

profile = "*****"
bucket_name = "*****"
region = "*****"
key_id = '*****'
secret_key = '*****'
AWS_AUDIO_LOCATOR = f'https://{bucket_name}.s3.{region}.amazonaws.com/'
os.environ['AWS_DEFAULT_REGION'] = region
os.environ['AWS_SHARED_CREDENTIALS_FILE'] = "."
session = boto3.session.Session(aws_access_key_id=key_id,aws_secret_access_key=secret_key)
s3 = session.resource('s3')
boto3.setup_default_session(aws_access_key_id=key_id,aws_secret_access_key=secret_key)
transcribe = boto3.client('transcribe', region_name=region)
s3_client = boto3.client('s3')
LOCALE = "es-ES"
colsdf=("Fichero", "Trabajo", "Timestamp", "Transcript")
jobsfilename = "TransAWSJobs.csv"
dire="."
numllamadas = 50000

viejojsons = os.listdir(dire)
nuevojsons = os.listdir(dire+"NuResults\\")
wavs = list()
jsons = list()

def damespeaker(pitems, pstart):
    if len(pitems) == 0:
        return "spk_0"
    for seg in pitems:
        for it in seg['items']:
            if it['start_time'] == pstart:
                return it['speaker_label']

def rehacer(pfich):
    wavfil = pfich[6:-4].replace("-","/")+"wav"
    print("Rehacer " + wavfil)
    resultsfilename = "TransAWSResults.csv"
    resultsurl = AWS_AUDIO_LOCATOR + resultsfilename
    df = pd.read_csv(resultsurl, sep=";", usecols=colsdf, index_col=0)
    df = df.drop(wavfil)
    df.to_csv(dire + resultsfilename, sep=";", encoding="utf_8", index=True)
    s3_client.upload_file(dire + resultsfilename, bucket_name, resultsfilename)
    s3.ObjectAcl(bucket_name, resultsfilename).put(ACL='public-read')
    return

corruptas = 0
while len(viejojsons) > len(nuevojsons)+3+corruptas:
    # List the object names
    nujson = {}
    for obj in viejojsons:
        filename = obj
        if filename.endswith(".json"):
            if filename in nuevojsons:
                #print(filename + " ya convertido")
                continue
            try:
                resjson = json.load(open(dire+filename, encoding="utf-8"))
                nujson['Trabajo'] = resjson['jobName']
                nujson['Transcripcion'] = resjson['results']['transcripts'][0]['transcript']
                nujson['Palabras'] = []
                palabras = resjson['results']['items']
                if len(palabras) and 'speaker_labels' in resjson['results'].keys():
                    items = resjson['results']['speaker_labels']['segments']
                else:
                    items = []
            except Exception as e:
                print(dire+filename + " Corrupto")
                rehacer(dire+filename)
                corruptas += 1
                continue
            #print(resjson['results']['speaker_labels']['segments'][0]['items'])
            for pal in palabras:
                if pal['type'] == 'pronunciation':
                    paldict={}
                    paldict['Timestamp']=pal['start_time']
                    paldict['Palabra'] = pal['alternatives'][0]['content']
                    paldict['Confianza'] = pal['alternatives'][0]['confidence']
                    paldict['Speaker'] = damespeaker(items, pal['start_time'])
                    nujson['Palabras'].append(paldict)
            with open(dire+"NuResults\\"+filename, 'w',encoding="utf-8") as outfile:
                json.dump(nujson, outfile,ensure_ascii=False)
            print("Creado " + dire+"NuResults\\"+filename)
    viejojsons = os.listdir(dire)
    nuevojsons = os.listdir(dire+"NuResults\\")

exit(0)

