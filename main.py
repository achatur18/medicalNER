import gradio
from pytube import YouTube
import os    
from glob import glob
import boto3
import time
import ast
import uuid 

import shutil

medclient = boto3.client('comprehendmedical', 'us-east-1')

s3 = boto3.client('s3')
c=0
transcribe_client = boto3.client('transcribe', region_name='ap-south-1')

def downloadYouTube(videourl, path):

    yt = YouTube(videourl)
    yt = yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').first()
    if not os.path.exists(path):
        os.makedirs(path)
    yt.download(path)


def uploadToS3(file_name, bucketName="textractbucketabhay", save_filename='input.mp4'):
    with open(file_name, "rb") as f:
        s3.upload_fileobj(f, bucketName, save_filename)
    return bucketName+"/"+save_filename


def transcribe(file_uri, job_name=str(uuid.uuid4().hex.upper())+str(c)):
    response = transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': file_uri},
                MediaFormat='mp4',
                LanguageCode='en-US'
                )
    # return response
    while True:
        response = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
        print(response['TranscriptionJob']['TranscriptionJobStatus'] )
        if response['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            return response
        time.sleep(5)

def extractDict(response_transcribe):
    import urllib.request
    url = response_transcribe['TranscriptionJob']['Transcript']['TranscriptFileUri']
    response = urllib.request.urlopen(url)
    data = response.read()      # a `bytes` object
    text = data.decode('utf-8') # a `str`; this step can't be used if data is binary
    text=ast.literal_eval(text)
    return text
    
def ner(text):
    response = medclient.detect_entities_v2(Text=text)
    return  response

def main(link='https://www.youtube.com/watch?v=zNyYDHCg06c', download_folder='./input'):
    global c
    c+=1
    # link='https://www.youtube.com/watch?v=zNyYDHCg06c'
    
    downloadYouTube(link, download_folder)
    print("download done!")

    file_name=glob( download_folder+"/*")[0]
    s3_filename=uploadToS3(file_name=file_name)

    s3_uri='s3://'+s3_filename
    print("upload done!", s3_uri)
    s3_uri="s3://textractbucketabhay/input.mp4"
    shutil.rmtree(download_folder)

    response_transcribe = transcribe(s3_uri)
    print("transcribe done!")
    

    if response_transcribe['TranscriptionJob']['TranscriptionJobStatus']=='FAILED':
        return {"status":"FAILED transcription"}

    response=extractDict(response_transcribe)

    result = ner(response['results']['transcripts'][0]['transcript'])
    print("type: ", type(result))
    d={}
    for res in result['Entities']:
        if res['Category'] in d.keys():
            d[res['Category']].append(res['Text'])
        else:
            d[res['Category']]=[res['Text']]
    
    return d

    
    

gradio.Interface(main, "text", "text").launch(share=True)