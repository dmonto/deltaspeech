import pandas as pd
import os
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

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


# Retrieve the bucket's objects
objects = list_bucket_objects(bucket_name)
wavs = list()
jsons = os.listdir("Caser\\NuResults")

if objects is not None:
    # List the object names
    hechas = 0
    yapedidas = 0
    for obj in objects:
        filename = obj
        if filename.endswith(".wav"):
            wavs.append(filename)
        #elif filename.endswith(".json"):
        #   jsons.append(filename[7:])
    print(f'{len(wavs)} Llamadas y hay {len(jsons)} Transcripciones')
    for wav in wavs:
        if (wav[:-4]+".json").replace("/","-") not in jsons:
            print("Falta " + wav)

exit(0)

