import pandas as pd
import os
import requests
import time
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import botocore

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
os.environ['AWS_SHARED_CREDENTIALS_FILE'] = "."
session = boto3.session.Session(aws_access_key_id=key_id,aws_secret_access_key=secret_key)
s3 = session.resource('s3')
boto3.setup_default_session(aws_access_key_id=key_id,aws_secret_access_key=secret_key)
transcribe = boto3.client('transcribe', region_name=region)
s3_client = boto3.client('s3')
LOCALE = "es-ES"
cols=("Fichero", "Trabajo", "Timestamp")
jobsfilename = "TransAWSJobs.csv"
dire="."
numllamadas = 50000

def awstranscribeasync(fil):
    job_name = "trans" + str(time.clock())
    aws_uri = AWS_AUDIO_LOCATOR + fil
    try:
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': aws_uri},
            MediaFormat='wav',
            LanguageCode=LOCALE,
            Settings = {
                'VocabularyName': 'Caser',
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': 2
            }
        )
    except Exception as e:
        print("Llegamos al limite")
        time.sleep(5)
        return ""

    return job_name


def list_bucket_objects(bucket_name):
    """List the objects in an Amazon S3 bucket

    :param bucket_name: string
    :return: List of bucket objects. If error, return None.
    """

    # Retrieve the list of bucket objects
    try:
        #response = s3.list_objects_v2(Bucket=bucket_name)
        buck = s3.Bucket(bucket_name).objects.all()
        response = []
        for obj in buck:
            response.append(obj.key)
    except ClientError as e:
        print(e)
        return None
    return response

jobsurl=AWS_AUDIO_LOCATOR + jobsfilename
df = pd.read_csv(jobsurl, sep=";")

# Retrieve the bucket's objects
objects = list_bucket_objects(bucket_name)
if objects is not None:
    # List the object names
    print(f'{len(objects)} Objects in {bucket_name}')
    hechas = 0
    yapedidas = 0
    for obj in objects:
        filename = obj
        filter = df[df["Fichero"]==filename]
        if len(filter) > 0:
            yapedidas += 1
            #print(f'  {filename} ya Pedido con trabajo {filter["Trabajo"].values[0]}')
            continue
        hechas += 1
        if hechas > numllamadas: break
        if filename.endswith(".wav"):
            now = str(datetime.now())
            res = awstranscribeasync(filename)
            if len(res) > 0:
                print(f'{filename} asignado {res}')
                lin = pd.DataFrame([[filename,res,now]], columns=cols)
                df = df.append(lin)
                if (hechas % 100) == 0:
                    print(f'{hechas + yapedidas} de {len(objects)} en proceso')
                    df.to_csv(dire + jobsfilename, sep=";", index=False)
                    s3_client.upload_file(dire + jobsfilename, bucket_name, jobsfilename)
                    s3.ObjectAcl(bucket_name, jobsfilename).put(ACL='public-read')

    print(f'{hechas + yapedidas} de {len(objects)} COMPLETADAS')
    df.to_csv(dire + jobsfilename, sep=";", index=False)
    s3_client.upload_file(dire + jobsfilename, bucket_name, jobsfilename)
    s3.ObjectAcl(bucket_name, jobsfilename).put(ACL='public-read')

