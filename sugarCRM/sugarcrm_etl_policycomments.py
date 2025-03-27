"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 21-may-2018
Usage  : "python sugarcrm_etl.py "CountryCode" "LoadType" "DateRange" "StartDate" "EndDate" "UniqueIdentifier""
          python sugarcrm_etl.py "AU" "Full" "" "" "" ""
          python sugarcrm_etl.py "AU" "Delta" "Yesterday-To-Now" "" "" ""
          python sugarcrm_etl.py "AU" "Delta" "Today" "" "" ""
          python sugarcrm_etl.py "AU" "Delta" "Last Hour and Current Hour" "" "" ""
          python sugarcrm_etl.py "AU" "Delta" "_User Defined" "2018-05-29" "2018-06-02" "null"
          python sugarcrm_etl.py "AU" "Test Delta" "Last Hour and Current Hour" "" "" "AU.CMA.AAA0033"

Parameters:      @CountryCode:	Required. Valid country code (AU, NZ, MY, SG, US, UK etc...)
					@LoadType:		Required. Valid values (Full, Delta, Test Full, Test Delta)
									If value is Full, the output is all consultants.
									If value is Delta, the output is all new/amended consultants. 
									If value is Test Full, the output is all test consultants.
									If value is Test Delta, the output is the account that is passed in @UniqueIdentifier parameter.
					@DateRange:		Required. Standard date range or "_User Defined". Default value 'Last Hour'
     				@StartDate:		Optional. Only enter if DateRange = _User Defined. Format: yyyy-mm-dd hh:mm:ss (eg. 2018-01-01 10:59:00)
					@EndDate:		Optional. Only enter if DateRange = _User Defined. Format: yyyy-mm-dd hh:mm:ss (eg. 2018-01-01 11:59:00)
					@UniqueIdentifier: Optional. Valid UniqueIdentifier value. Eg. 'AU.CMA.66A61V.FionaGilbert'
         
Desc   : 

Configuration requirements : 
    
Change History:

Pending Bits: 



"""
#import pyodbc 
import json
import datetime
#import sys#for initial testing
#import sugarcrm_generic_module
import requests

import sugarcrm_generic_module
# 20220708      CHG0036132 decommissioned the proxy servers, and due to being unable to affect the system environment variables that CmdExec job steps use, 
# this workaround removes proxy info temporarily to avoid a connection error
import os
os.environ["HTTPS_PROXY"] = ""
os.environ["HTTP_PROXY"] = ""

 #------------------------------------------------------------
#Notes/Policy Comments data loading routine
def load_notes_data():
 try:
     #Notes/PolicyComments delta/differential load.
       print('Notes/policycomments data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW started @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       sugarcrm_generic_module.cursor.execute("exec dbo.rptsp_rpt0999 '"+sugarcrm_generic_module.vCountryCode+"','"+sugarcrm_generic_module.vLoadType+"','"+sugarcrm_generic_module.vDateRange
                      +"','"+sugarcrm_generic_module.vStartDate+"','"+sugarcrm_generic_module.vEndDate+"','"+sugarcrm_generic_module.vUniqueIdentifier+"'");
       print('Notes/policycomments data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW completed @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       print(sugarcrm_generic_module.vCountryCode+' notes/policy comments data push to SugarCRM started @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       loop_counter=0
       for row in sugarcrm_generic_module.cursor:
          #continue
          #vaccount_integration_id_c=row.UniqueIdentifier
          vpolicy_comment_id_c=row.CallCommentID
          #print(row.CommentDateTime)
          #print(str(datetime.datetime.strftime(row.CommentDateTime,'%Y-%m-%d %H:%M:%S')))
          #print(datetime.datetime.strftime(row.CommentDateTime,'%Y-%m-%d %H:%M:%S'))
          #sys.exit()
          jsonNotesrecord={"account_integration_id_c":row.UniqueIdentifier,
                           "policy_number_c":row.PolicyNumber,
                           "policy_comment_id_c":row.CallCommentID,
                            "comment_date_time_c":datetime.datetime.strftime(row.CommentDateTime,'%Y-%m-%dT%H:%M:%S+10:00'),
                            "username_c":row.User,
                            "description":row.Comment
                   }
          #print(jsonNotesrecord)
          #sys.exit()
          url = sugarcrm_generic_module.rest_url+"Notes/filter"
          #payload = {"filter":[{"$or":[{"account_integration_id_c":{"$equals":vaccount_integration_id_c}}]}],"fields":"id"}
          payload = {"filter":[{"$or":[{"policy_comment_id_c":{"$equals":vpolicy_comment_id_c}}]}],"fields":"id"}
          #print(payload)
          payload = json.dumps(payload)
          sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
          headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
          response = requests.request("GET", url, data=payload, headers=headers)
          sugarcrm_generic_module.checkApiCallSuccess(response,'Notes/PolicyComment existence check failed.')
          if len(response.json()["records"])==0:
            print('Notes/PolicyComment doesnt exist. Inserting')
            url = sugarcrm_generic_module.rest_url+"Notes"
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonNotesrecord)
            response = requests.request("POST", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Note/PolicyComment creation failed for payload:'+payload)
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmPolicyComment] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+row.UniqueIdentifier
                                                          +"' and PolicyNumber='"+row.PolicyNumber+"' and CallCommentID='"+row.CallCommentID+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('--Note/PolicyComment created successfully. vpolicy_comment_id_c:'+vpolicy_comment_id_c)
          elif len(response.json()["records"])>1:
            print('Multiple account records found. Raise error')
          elif len(response.json()["records"])==1:
            print('Note/PolicyComment exists. Updating.')
            sugarUniqId=response.json()["records"][0]["id"]
            url = sugarcrm_generic_module.rest_url+"Notes/"+sugarUniqId
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonNotesrecord)
            response = requests.request("PUT", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Note/PolicyComment update failed for payload:'+payload)
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmPolicyComment] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+row.UniqueIdentifier
                                                          +"' and PolicyNumber='"+row.PolicyNumber+"' and CallCommentID='"+row.CallCommentID+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('Note/PolicyComment updated successfully. vpolicy_commment_id_c:'+vpolicy_comment_id_c)
          else:
            raise;
          loop_counter=loop_counter+1
          #if loop_counter > 3:
            #sys.exit()
       print('Total Notes/PolicyComment records processed:'+str(loop_counter))
       print(sugarcrm_generic_module.vCountryCode+' notes/policycomments data push to SugarCRM successfully completed @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
 except Exception as excp:
    sugarcrm_generic_module.update_cursor.rollback()
    sugarcrm_generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    sugarcrm_generic_module.update_cnxn.close() 
    raise Exception("Error while executing load_notes_data. Full error description is:"+str(excp))
#-------------------------------------------------------------------------------------
#Main processing block
print('***************************************************************************')
print('Main processing block started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
sugarcrm_generic_module.validateParams()
sugarcrm_generic_module.access_token=sugarcrm_generic_module.get_token()
sugarcrm_generic_module.connect_db()

vStartTimestamp=datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(hours=1),'%Y-%m-%d %H:00:00')
vEndTimestamp=datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(hours=1),'%Y-%m-%d %H:59:59')

#Notes/PolicyComments Data load block | Module_name is "Notes"
load_notes_data()

#cursor.close() 
sugarcrm_generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
sugarcrm_generic_module.update_cnxn.close()
print('Main processing block finished at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))