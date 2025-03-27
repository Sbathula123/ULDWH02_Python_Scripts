"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 21-May-2018
Desc   : This is generic module contains common code.
         This code is not called directly but via other scripts.

Configuration requirements:

Pending Bits: 

"""

#import required libraries
import requests
import json
import pyodbc 
import sys
import datetime
from tqdm import tqdm

#import io
import os

global vAudioFilesDir
if os.getenv('COMPUTERNAME','defaultValue')=='ULDWH02':
    vAudioFilesDir = 'E:\\ETL\\Python Scripts\\VoiceAnalytics\\AudioFiles\\'
    vGCPCredentialsFileDir = 'E:\\ETL\\Python Scripts\\VoiceAnalytics\\Credentials\\'
elif os.getenv('COMPUTERNAME','defaultValue')=='DESKTOP-PC0002':
    vAudioFilesDir = 'C:\\temp\\'
    vGCPCredentialsFileDir = 'C:\\temp\\'
else:
    vAudioFilesDir = 'C:\\temp\\'
    vGCPCredentialsFileDir = 'C:\\temp\\'



# Imports the Google Cloud client library
#from google.cloud import speech
from google.cloud import speech_v1p1beta1 as speech
#from google.cloud.speech import enums
#from google.cloud.speech import types
#from google.cloud.speech_v1p1beta1 import enums
#from google.cloud.speech_v1p1beta1 import types

from google.cloud import storage

#to convert audio format from sterio to mono
from pydub import AudioSegment

#Variable declaration
rest_url="http://10.2.30.16/api/rest/"

# define user-defined exceptions
class Error(Exception):
   """Base class for other exceptions"""
   pass

class DbConnectionError(Error):
   """Raised when the Database connection fails"""
   pass

class ApiCallError(Error):
   """Raised when api call fails due to various reasons including connectivity/credentials"""
   pass


#-------------------------------------------------------------------------------------
#Check if valid parameters are passed
def validateParams():
  #sys.argv will be 7 if 6 parameters are passed
 if len(sys.argv)<7:
   raise Exception('Please execute the script with all valid parameters "CountryCode" "LoadType" "DateRange" "StartDate" "EndDate" "UniqueIdentifier"')
 if len(sys.argv)>=7:
  global  vCountryCode
  global  vLoadType
  global  vDateRange
  global  vStartDate
  global  vEndDate
  global  vUniqueIdentifier
  vCountryCode=sys.argv[1]
  vLoadType=sys.argv[2]
  vDateRange=sys.argv[3]
  vStartDate=sys.argv[4]
  vEndDate=sys.argv[5]
  vUniqueIdentifier=sys.argv[6]
  #print('Passsed values are CountryCode:'+vCountryCode+'*LoadType:'+vLoadType+'*DateRange:'+vDateRange+'*StartDate:'+vStartDate+'*EndDate:'+vEndDate+'*UniqueIdentifier:'+vUniqueIdentifier)
 if vCountryCode != 'AU':
   raise Exception('Script is currently supported for AU region only')
 elif vLoadType not in ['Full', 'Delta', 'Test Full', 'Test Delta']:
   raise Exception('Invalid LoadType passed:'+vLoadType)
 print('Parameter validation successful. Passsed values are CountryCode:'+vCountryCode+'*LoadType:'+vLoadType+'*DateRange:'+vDateRange+'*StartDate:'+vStartDate+'*EndDate:'+vEndDate+'*UniqueIdentifier:'+vUniqueIdentifier)
 
#-------------------------------------------------------------------------------------
#MS SQL Server Connectivity and session/cursor creation     
def connect_db():
 try:
     global cnxn;
     cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server=ULDWH02;"
                      "Database=db-au-cmdwh;"
                      "Trusted_Connection=yes;")
     global cursor;
     cursor = cnxn.cursor()
     cursor.rollback();
     
     global update_cnxn;
     update_cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server=ULDWH02;"
                      "Database=db-au-cmdwh;"
                      "Trusted_Connection=yes;")
     global update_cursor;
     update_cursor = update_cnxn.cursor()
     update_cursor.rollback();
     print('Database connection successful.')
 except Exception as excp:
     raise Exception("Error while Connecting to database. Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#Check if API call was successful
def checkApiCallSuccess(response,errorMessage,raiseErrorAndStop):
  if str(response) != '<Response [200]>':
    print('checkApiCallSuccess failed. API Response is as below:')
    print(response.text)
    if raiseErrorAndStop=='Y':
     print('Reciprocating above exception to main() and stopping execution.')
     raise Exception(errorMessage)
    else:
     return 'F'
  else:
      return 'S'
      
#-------------------------------------------------------------------------------------
'''http_proxy  = "http://10.2.20.10:8080"
https_proxy = "https://10.2.20.10:8080"
ftp_proxy   = "ftp://10.2.20.10:8080"
proxyDict = { 
              "http"  : http_proxy, 
              "https" : https_proxy,
              "ftp"   : ftp_proxy
            }
'''
#-------------------------------------------------------------------------------------
#Authentication routine
def authenticate():
   try: 
     url = rest_url+"authorize"
     payload = "[\r\n{\r\n\"id\":\"recording\",\r\n\"data\":\r\n{\r\n\"domain\":\"covermore\",\r\n\"locale\":\"en\"\r\n},\r\n\"userId\":\"appbobj\",\r\n\"password\":\"@!!b0bj\",\r\n\"locale\":\"en\"\r\n}\r\n]\r\n"
     '''headers = {
                'Authorization': "Basic YXBwYm9iajpAISFiMGJq",
                'Cache-Control': "no-cache",
                'Postman-Token': "9ac72598-524f-4a49-afad-11bae71f31d7"
                }'''
     #authResponse = requests.request("POST", url, data=payload, headers=headers)#,proxies=proxyDict
     #authResponse = requests.request("POST", url, data=payload)#,proxies=proxyDict
     session=requests.Session()
     authResponse = session.request("POST", url, data=payload)#,proxies=proxyDict
     global cookie
     cookie=session.cookies.get_dict()
     #print(cookie)
     checkApiCallSuccess(authResponse,'Authorization failed.','Y')
     #auth_json_data = json.loads(authResponse.text)
     #print(auth_json_data)
     #access_token=auth_json_data["access_token"] 
     print('Authorization successful.')
     #return access_token
   except ApiCallError as excp:
     print(str(excp))
     raise;
   except Exception as excp:
     raise Exception("Error while authentication. Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#Save transcript to database
#def save_transcript_to_database(MetaDataID,):


#Get call metadata and wav file URL
def get_wave_file_url(MetaDataID):
 try: 
     url = rest_url+"recording/contact/"+MetaDataID+"/recording?dojo.preventCache=UNIX_TIME"
     #print(url)
     '''headers = {
        'Cache-Control': "no-cache",
        'Postman-Token': "9e9611c9-c77d-43ef-9363-52dabfa53497"
        }'''
     #querystring = {"dojo.preventCache":"UNIX_TIME"}
     #print('debug 1'+str(cookie))
     #authResponse = requests.request("GET", url,headers=headers,params=querystring)#,proxies=proxyDict
     authResponse = requests.request("GET", url,cookies=cookie)#,proxies=proxyDict
     #print(authResponse.text)#for testing
     if checkApiCallSuccess(authResponse,'Recording Not found.','N')=='S':
       auth_json_data = json.loads(authResponse.text)
       #print(auth_json_data)
       vWavFileURL=auth_json_data['wav']['playUrl']
       return vWavFileURL
     else:
       return None
 except ApiCallError as excp:
     print(str(excp))
 except Exception as excp:
     raise Exception("Error occurred during . Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#Get call metadata and wav file URL
def convert_voice_to_text(MetaDataID):
   try: 
     #print(MetaDataID)
     #sys.exit()
     '''url = rest_url+"recording/contact/"+MetaDataID+"/recording?dojo.preventCache=UNIX_TIME"
     #print(url)
     #querystring = {"dojo.preventCache":"UNIX_TIME"}
     #print('debug 1'+str(cookie))
     #authResponse = requests.request("GET", url,headers=headers,params=querystring)#,proxies=proxyDict
     authResponse = requests.request("GET", url,cookies=cookie)#,proxies=proxyDict
     checkApiCallSuccess(authResponse,'Recording Not found.','N')
     auth_json_data = json.loads(authResponse.text)
     #print(auth_json_data)
     vWavFileURL=auth_json_data['wav']['playUrl']'''
     vWavFileURL=get_wave_file_url(MetaDataID)
     if vWavFileURL!=None:
         #Define audio file names on local and cloud
         vWavFileNameOrigLocal=vAudioFilesDir+vWavFileURL[vWavFileURL.index('play/')+5:vWavFileURL.index('.wav')]+'_Orig.wav'
         #vWavFileNameMonoLocal=vAudioFilesDir+vWavFileURL[vWavFileURL.index('play/')+5:vWavFileURL.index('.wav')]+'_Mono.wav'
         vWavFileNameOrigBucket='DataLake/VoiceAnalytics/'+vWavFileURL[vWavFileURL.index('play/')+5:vWavFileURL.index('.wav')]+'_Orig.wav'
         #vWavFileNameMonoBucket='DataLake/VoiceAnalytics/'+vWavFileURL[vWavFileURL.index('play/')+5:vWavFileURL.index('.wav')]+'_Mono.wav'
         
         wavResponse = requests.request("GET",vWavFileURL,cookies=cookie,stream=True)
         print('Streaming audio from cisco for MetaDataID:'+MetaDataID+' started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
         
         #open file in bytes mode and write to a file
         with open(vWavFileNameOrigLocal, 'wb') as f:
          #for data in tqdm(wavResponse.iter_content()):#tqdm means streaming mode. Each stream screenshot is written in the log file and increases logsize unnecessarily.
          for data in wavResponse.iter_content():#in direct mode no streaming.
            f.write(data)
         f.close()
               
         #convert audio file format from stereo to mono not required since google api has started supporting the stereo format file.
         #sound = AudioSegment.from_wav(vWavFileNameOrigLocal)
         #sound = sound.set_channels(1)
         #sound.export(vWavFileNameMonoLocal, format="wav")
         #print('file converted to mono format')
       
         #Passing google cloud credentials
         os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = vGCPCredentialsFileDir+"Cover-More Data and Analytics-fd15ac400401.json"
         
          
         #Instantiates a speech client
         speechclient = speech.SpeechClient()
         
         #instantiate a storage client for file upload to GCP bucket
         print('File upload to GCP Bucket started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
         storageclient =storage.Client()
         bucket = storageclient.get_bucket('cmbi-au')
         blob = bucket.blob(vWavFileNameOrigBucket)
         blob.upload_from_filename(vWavFileNameOrigLocal)
         #blob = bucket.blob(vWavFileNameMonoBucket)
         #blob.upload_from_filename(vWavFileNameMonoLocal)
         
         #removing the local files after upload
         os.remove(vWavFileNameOrigLocal)
         #os.remove(vWavFileNameMonoLocal)
         
         print('GCP file upload Completed at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
         # Loads the audio into memory not required since it will be read from GCP bucket now due to larger file sizes.
         '''with io.open(vWavFileName, 'rb') as audio_file:
             content = audio_file.read()
             audio = types.RecognitionAudio(content=content)'''
         #audio = speech.types.RecognitionAudio(uri='gs://cmbi-au/'+vWavFileNameMonoBucket)
         audio = speech.types.RecognitionAudio(uri='gs://cmbi-au/'+vWavFileNameOrigBucket)#for multi channel stereo audio file only
         #print('audio loaded in memory')
        #for all our cisco files this is the encoding that is actually working.
         '''config = speech.types.RecognitionConfig(
            #encoding=speech.enums.RecognitionConfig.AudioEncoding.LINEAR16,
            sample_rate_hertz=8000,
            language_code='en-US',
            use_enhanced=True,
            model='phone_call'        
            )'''
         config = speech.types.RecognitionConfig(
            #encoding=speech.enums.RecognitionConfig.AudioEncoding.LINEAR16,
            audio_channel_count=2,
            enable_automatic_punctuation=True,
            enable_separate_recognition_per_channel=True,
            #sample_rate_hertz=8000,
            language_code='en-US',#as on 20-aug-2018 got error if I change language to AU "The phone_call model is currently not supported for language : en-AU"
            use_enhanced=True,
            model='phone_call'        
            )
             
         #print('defined config')
         operation = speechclient.long_running_recognize(config, audio)
         #print('defined operation')
         # Detects speech in the audio file
         #response = client.recognize(config, audio)
         #response = operation.result(timeout=90) 
         print('Transcribing audio.... trascription process started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
         response = operation.result(timeout=900)#timeout value changed by Ratnesh on 17-aug-2018
         print('Transcription complete at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
         print('****************FULL TRANSCRIPT START*************METADATAID:'+MetaDataID)
         print(response.results)
         print('****************FULL TRANSCRIPT END*************METADATAID:'+MetaDataID)
         for result in response.results:
          vTranscriptText='Transcript : {}'.format(result.alternatives[0].transcript)
          vConfidenceLevel=result.alternatives[0].confidence
          vCustomerTranscriptText='Customer Transcript :'+result.alternatives[0].transcript if result.channel_tag==1 else ''
          vEmployeeTranscriptText='Employee Transcript :'+result.alternatives[0].transcript if result.channel_tag==2 else ''
          #print(vTranscriptText)
          try:
               vQueryString="insert into [db-au-cmdwh]..cisCallTranscription(MetaDataID,Transcript,confidence_level,CustomerTranscript,EmployeeTranscript,InsertDateTime) Values(?,?,?,?,?,?)"
               #print(vQueryString)
               update_cursor.execute(vQueryString,(MetaDataID,vTranscriptText,vConfidenceLevel,vCustomerTranscriptText,vEmployeeTranscriptText,str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S.%f'))[:-3]));
               update_cursor.commit();
               #print('commit complete')
          except Exception as excp:
              raise Exception("Error while saving transcript. Full error description is:"+str(excp))
   except ApiCallError as excp:
     print(str(excp))
     raise;
   except Exception as excp:
     raise Exception("Error while executing convert_voice_to_text. Full error description is:"+str(excp))




#-------------------------------------------------------------------------------------
#Run a loop and process
def process_call_metadata():
 try:
       #comment
       vQueryString="select MetaDataID,LocalStartTime from [db-au-cmdwh]..cisCallMetaData WITH (NOLOCK) \
                       where LocalStartTime >= cast(dateadd(day,-2,getdate()) as date) \
                       and MetaDataID not in (select MetaDataID from [db-au-cmdwh]..cisCallTranscription) \
                       and duration > 1000\
                       and team!='CN Medical Assistance'\
                       order by LocalStartTime"
                       #10762234 test case for large datafile
       #print(vQueryString)
       cursor.execute(vQueryString);
       print('cursor executed')
       for row in cursor:
           print('Processing Call MetaDataID:'+str(row.MetaDataID)+' started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')));
           try:
               convert_voice_to_text(str(row.MetaDataID));
           except Exception as excp:
               print("Error while executing convert_voice_to_text for:"+str(row.MetaDataID)+"Full error description is:"+str(excp))
           print('Processing Call MetaDataID:'+str(row.MetaDataID)+' completed at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')));
 except Exception as excp:
     cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
     update_cnxn.close();
      #sys.stdout.close()
     raise Exception("Error while executing process_call_metadata() Full error description is:"+str(excp))