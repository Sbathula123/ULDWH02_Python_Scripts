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
# 20220708      CHG0036132 decommissioned the proxy servers, and due to being unable to affect the system environment variables that CmdExec job steps use, 
# this workaround removes proxy info temporarily to avoid a connection error
import os
os.environ["HTTPS_PROXY"] = ""
os.environ["HTTP_PROXY"] = ""

#Variable declaration
access_token=''
rest_url="https://covermore.sugarondemand.com/rest/v11_1/"


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
                      "Database=db-au-ods;"
                      "Trusted_Connection=yes;")
     global cursor;
     cursor = cnxn.cursor()
     cursor.rollback();
     
     global update_cnxn;
     update_cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server=ULDWH02;"
                      "Database=db-au-ods;"
                      "Trusted_Connection=yes;")
     global update_cursor;
     update_cursor = update_cnxn.cursor()
     update_cursor.rollback();
     print('Database connection successful.')
 except Exception as excp:
     raise Exception("Error while Connecting to database. Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#Check if API call was successful
def checkApiCallSuccess(response,errorMessage):
  if str(response) != '<Response [200]>':
    print('checkApiCallSuccess failed. API Response is as below:')
    print(response.text)
    print('Reciprocating above exception to main() and stopping execution.')
    raise Exception(errorMessage)
      
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
#Authentication Token generation routine
def get_token():
   try: 
     url = rest_url+"oauth2/token"
     payload = "{ \r\n\"grant_type\":\"password\",\r\n\"client_id\":\"sugar\",\r\n\"client_secret\":\"\",\r\n\"username\":\"dw.api.user\",\r\n\"password\":\"sCP1he0yoWFixa4X\",\r\n\"platform\":\"dwapi\" \r\n}\r\n"
     headers = {
        'Authorization': "Bearer 07531fd2-7d86-4f7c-8204-def96470a6e1",
        'Cache-Control': "no-cache",
        'Postman-Token': "ca25a808-71f0-4e6c-b3e0-c04f3f8754fd"
        }
     authResponse = requests.request("POST", url, data=payload, headers=headers)#,proxies=proxyDict
     checkApiCallSuccess(authResponse,'Token generation failed.')
     auth_json_data = json.loads(authResponse.text)
     access_token=auth_json_data["access_token"] 
     print('Token Generation successful. access Token is:'+access_token)
     return access_token
   except ApiCallError as excp:
     print(str(excp))
     raise;
   except Exception as excp:
     raise Exception("Error while generating token. Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#Check Token validity routine (return same token if active, if expired, then regenerate and return new token)
def validate_token(access_token):
 try: 
   url = rest_url+"ping"
   #payload = "{ \r\n\"grant_type\":\"password\",\r\n\"client_id\":\"sugar\",\r\n\"client_secret\":\"\",\r\n\"username\":\"dw.api.user\",\r\n\"password\":\"sCP1he0yoWFixa4X\",\r\n\"platform\":\"dwapi\" \r\n}\r\n"
   headers = {
      'oauth-token': access_token,
      'Content-Type': "application/x-www-form-urlencoded",
      'Cache-Control': "no-cache",
      'Postman-Token': "980135a2-06da-46b1-983d-801164b44f9e"
      }
   authResponse = requests.request("GET", url, headers=headers)#,proxies=proxyDict
   #checkApiCallSuccess(authResponse,'Token validation ping operation failed.')
   if authResponse.text == '"pong"':
    return access_token
   else:
    print('Token expired. Regenerating')
    access_token=get_token()
    return access_token
 except ApiCallError as excp:
     print(str(excp))
     raise;
 except Exception as excp:
     raise Exception("Error while Validating token. Full error description is:"+str(excp))
     validate_token('sdfd')
#-------------------------------------------------------------------------------------
#Authentication Token refresh routine
#def refresh_token():
#get_token can be used and refresh functionality is actually not required.

#-------------------------------------------------------------------------------------
#get module ids (unique identifiers generated by/from sugarcrm) for deletion purpose
def get_module_id(access_token,module_name):
 try:
    url = rest_url+module_name
    #payload =''
    #payload ={ "max_num": 500,	"fields": "id" }
    payload ={ "max_num": 1000,	"fields": "id" } #dont increase it more than 500 else it starts taking longer. it takes 1 min for 500 but 3 mins for 1000 records
    headers = {
      'oauth-token': access_token,
      'Content-Type': "application/json",
      'Cache-Control': "no-cache",
      'Postman-Token': "512c6da2-6636-4ab3-a06c-e842565a029c"
      }
    response = requests.request("GET", url, data=json.dumps(payload), headers=headers)
    checkApiCallSuccess(response,'get module id call failed.')
    return json.loads(response.text)
 except ApiCallError as excp:
     print(str(excp))
     raise;
 except Exception as excp:
     raise Exception("Error while getting module id. Full error description is:"+str(excp))
#---------------------------------------------------------------------------------------
'''
#Accounts records cleanup routine
#this will not delete if there are less than 20 accounts found.
def delete_module_data_using_api(access_token,module_name):
 print('Deleting data using api')
 resultdict=get_module_id(access_token,module_name)
 #resultdict["records"][0]["id"]
 #resultdict["records"][1]["id"]
 #for i in resultdict["records"]:
 #payload =''
 headers = {
    'oauth-token': access_token,
    'Content-Type': "application/json",
    'Cache-Control': "no-cache",
    'Postman-Token': "512c6da2-6636-4ab3-a06c-e842565a029c"
    }

 vdelete_counter=0
 while resultdict["next_offset"] == 20:
  for i in resultdict["records"]:
    #print(i["id"])
    url = rest_url+module_name+"/"+i["id"]
    #print('deleting id:'+i["id"]+'using url:'+url)
    #response = requests.request("DELETE", url, data=payload, headers=headers)
    print(str(vdelete_counter)+' deleting '+url)
    #response = requests.request("DELETE", url, headers=headers)
    requests.request("DELETE", url, headers=headers)
    vdelete_counter=vdelete_counter+1
    #print(response.text+' deleted and counter is:'+str(vdelete_counter))
  access_token=validate_token(access_token)
  headers = {
    'oauth-token': access_token,
    'Content-Type': "application/json",
    'Cache-Control': "no-cache",
    'Postman-Token': "512c6da2-6636-4ab3-a06c-e842565a029c"
    }
  resultdict=get_module_id(access_token,module_name)
'''  
 #------------------------------------------------------------
#delete specific module data in batches of 20 records.
def delete_module_data_using_bulk_api(access_token,module_name):
 try:
   jsonModuleDeleteList=[]
   access_token=validate_token(access_token)
   resultdict=get_module_id(access_token,module_name)
   #print('resultdict in delete_module_data_using_bulk_api is:'+str(resultdict))
   #sys.exit()
   vdelete_counter=0
   #while resultdict["next_offset"] != -1:
   while (True):
    for i in resultdict["records"]:
     #print('id is :'+i["id"]+' and counter is '+str(vdelete_counter))
     jsonModuleDeleteRequest={"url":"/v10/"+module_name+"/"+i["id"],"method":"DELETE"}
     jsonModuleDeleteList.append(jsonModuleDeleteRequest)
     #print('jsonModuleDeleteList is :'+str(jsonModuleDeleteList))
     vdelete_counter=vdelete_counter+1
    #if vdelete_counter>=19:#we can run for max 20 records in a go otherwise before deleting if we fetch the next set of id, then same result will be returned.
     #print('in if part')
    jsonModuleBulkRequest={"requests":jsonModuleDeleteList}
    jsonModuleBulkRequest=json.dumps(jsonModuleBulkRequest)
    access_token=validate_token(access_token)
    write_data_using_bulk_api(jsonModuleBulkRequest,access_token)
    print('Batch deletion for '+str(vdelete_counter)+' records successful.')
    if resultdict["next_offset"] ==-1: #if last loop(-1) otherwise regular loop with 20 recrods
        print('Total records deleted:'+str(vdelete_counter))
        return
    jsonModuleDeleteList=[]
    access_token=validate_token(access_token)
    resultdict=get_module_id(access_token,module_name)
 except Exception as excp:
    raise Exception("Error while executing delete_module_data_using_bulk_api. Full error description is:"+str(excp))  
    #print('resultdict in delete_module_data_using_bulk_api is:'+str(resultdict))
    
     #vdelete_counter=0
     #sys.exit()
    #else:
     #print('in else part')#should never come to this part.
     #resultdict=get_module_id(access_token,module_name)
     #vdelete_counter=0
          
   #print('Deleting remaining records')     
   #jsonAccountBulkRequest={"requests":jsonAccountDeleteList}
   #jsonAccountBulkRequest=json.dumps(jsonAccountBulkRequest)
   #print(jsonAccountBulkRequest)
   #write_data_using_bulk_api(jsonAccountBulkRequest,access_token)
      
#-------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------
def write_data_using_bulk_api(jsonAccountBulkRequest,access_token):
 try:
   #print('Writing data using bulk api')
   url = rest_url+"bulk"
   payload =jsonAccountBulkRequest
   headers = {
      'oauth-token': access_token,
      'Content-Type': "application/json",
      'Cache-Control': "no-cache",
      'Postman-Token': "512c6da2-6636-4ab3-a06c-e842565a029c"
      }
   response = requests.request("POST", url, data=payload, headers=headers)
   #print(response.text)
   checkApiCallSuccess(response,'Bulk write failed.')
   print('Bulk batch commit checkpoint.'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
 except ApiCallError as excp:
     print(str(excp))
     raise;
 except Exception as excp:
     raise Exception("Error while executing write_data_using_bulk_api. Full error description is:"+str(excp))
