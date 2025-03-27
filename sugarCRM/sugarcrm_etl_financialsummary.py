"""
Author : Ratnesh
Dept   : Data Analytics & Innovation, Covermore Group
Date   : 21-may-2018
Usage  : "python sugarcrm_etl.py "CountryCode" "LoadType" "DateRange" "StartDate" "EndDate" "UniqueIdentifier""
          python sugarcrm_etl.py "AU" "Full" "" "" "" ""
          python sugarcrm_etl.py "AU" "Delta" "Today and Tomorrow" "" "" ""
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
         
Desc   : This is ETL module for SugarCRM. It utilizes the API framework for interaction with SugarCRM.
             1) Gets Accounts, Consultants and Financial Data from EDW.
             2) Prepares a json object with the data and then writes it to SugarCRM using API call.
             3) Before each commit execution it checks the validity of the token and if expired, it regenerates a new token.

Configuration requirements : 1) Deployment of this script.
                             2) Scheduling using agent for everyday execution (7 days a week) at TBD AM. using command shell or power shell.
                             3) 

Change History:
20180521        RATNESH          Proxy was causing some connection issues so proxy related code was commented out. Proxy was added at server level in advanced settings.
20180606        RATNESH          Shipping and Billing addressed were flipped based on request from Suchita.


Pending Bits: 
Check if any parallel instance of process is running.
COnsultant clean up on full load to be enabled.


"""
#import pyodbc 
import json
import datetime
import sys#for initial testing
#import sugarcrm_generic_module
import requests

import sugarcrm_generic_module
# 20220708      CHG0036132 decommissioned the proxy servers, and due to being unable to affect the system environment variables that CmdExec job steps use, 
# this workaround removes proxy info temporarily to avoid a connection error
import os
os.environ["HTTPS_PROXY"] = ""
os.environ["HTTP_PROXY"] = ""

#------------------------------------------------------------
#Financial target data loading routine
def load_financialtarget_data():
 try:
   if sugarcrm_generic_module.vLoadType=='Full':
     #Full Financial Target data load block
     print('*****DISABLED******************************Deleting Financial Target data from SugarCRM')
     #sugarcrm_generic_module.delete_module_data_using_bulk_api(sugarcrm_generic_module.access_token,"COV_FinancialSummary")
     #sys.exit()
     print('Financial Target data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW started @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
     sugarcrm_generic_module.cursor.execute("exec dbo.rptsp_rpt0997 '"+sugarcrm_generic_module.vCountryCode+"','"+sugarcrm_generic_module.vLoadType+"','"+sugarcrm_generic_module.vDateRange+"','"+sugarcrm_generic_module.vStartDate+"','"+sugarcrm_generic_module.vEndDate+"','"+sugarcrm_generic_module.vUniqueIdentifier+"'");
     print('Financial Target data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW completed @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
     print(sugarcrm_generic_module.vCountryCode+' Financial Target data push to SugarCRM started @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
     jsonFTRequestList=[]
     total_count=0
     loop_counter=0
     for row in sugarcrm_generic_module.cursor:
        jsonFTrecord={"account_integration_id_c":row.UniqueIdentifier,
                            #"policy_count_actual_c":row.,
                            #"quote_count_actual_c":row.,
                            #"gross_sales_actual_c":row.,
                            #"currency_code_c":row.,
                            #"commission_actual_c":row.,
                            #"strike_rate_c":row.,
                            "sales_target_c":row.SalesTarget,
                            "policy_count_target_c":row.PolicyCount,
                            "currency_code_target_c":row.CurrencyCode
                            #"commission_target_c":row.,
                            #"financial_month_c":row.,
                            #"financial_year_c":row.,
                            #"name":row.
                    }
        #print(jsonFTrecord)
        #jsonFTCreateRequest={"url":"/v11_1/COV_FinancialSummary","method":"POST","data":jsonFTrecord}
        jsonFTCreateRequest={"url":"/v10/COV_FinancialSummary","method":"POST","data":jsonFTrecord}
        jsonFTRequestList.append(jsonFTCreateRequest)
        loop_counter=loop_counter+1
        sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmFinancialTarget] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+row.UniqueIdentifier.replace("'","''")+"'")
        #if loop_counter>3:#for testing debugging only
        #     break;
        #update_cursor.commit()
        #print('row= %r' % (row,))
        if loop_counter >= 300:
         #print(jsonFTRequestList)
         jsonFTBulkRequest={"requests":jsonFTRequestList}
         jsonFTBulkRequest=json.dumps(jsonFTBulkRequest)
         sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
         sugarcrm_generic_module.write_data_using_bulk_api(jsonFTBulkRequest,sugarcrm_generic_module.access_token)
         sugarcrm_generic_module.update_cursor.commit()
         #print('batch written')
         total_count=total_count+loop_counter
         print('Loading batch for '+str(total_count)+' records successful.')
         loop_counter=0
         jsonFTRequestList=[]
         #sys.exit()
     print('Loading remaining records')
     jsonFTBulkRequest={"requests":jsonFTRequestList}
     jsonFTBulkRequest=json.dumps(jsonFTBulkRequest)
     sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
     sugarcrm_generic_module.write_data_using_bulk_api(jsonFTBulkRequest,sugarcrm_generic_module.access_token)
     sugarcrm_generic_module.update_cursor.commit()
     if loop_counter <= 300:
      total_count=total_count+loop_counter
     print('Total FT loaded:'+str(total_count))
     print(sugarcrm_generic_module.vCountryCode+' financial target data push to SugarCRM successfully completed @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
   else:
     #Fiancial Trade delta/differential load.
       print('Financial Trade data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW started @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       sugarcrm_generic_module.cursor.execute("exec dbo.rptsp_rpt0997 '"+sugarcrm_generic_module.vCountryCode+"','"+sugarcrm_generic_module.vLoadType+"','"+sugarcrm_generic_module.vDateRange+"','"+sugarcrm_generic_module.vStartDate+"','"+sugarcrm_generic_module.vEndDate+"','"+sugarcrm_generic_module.vUniqueIdentifier+"'");
       #cursor.execute("exec dbo.rptsp_rpt0994 '"+vcountry_code+"',null,'Full'");
       #cursor.execute("exec dbo.rptsp_rpt0994 '"+'AU'+"','AU.CMA.AAN2400','Test'");
       #cursor.execute("exec dbo.etlsp_ETL099_scrmAccount '"+'AU'+"','AU.CMA.CTN0148'");
       print('Financial Target data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW completed @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       print(sugarcrm_generic_module.vCountryCode+' Financial Target data push to SugarCRM started @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       loop_counter=0
       for row in sugarcrm_generic_module.cursor:
          #continue
          vfinacial_integration_id_c=row.UniqueIdentifier
          jsonFTrecord={"account_integration_id_c":row.UniqueIdentifier,
                           #"policy_count_actual_c":row.,
                            #"quote_count_actual_c":row.,
                            #"gross_sales_actual_c":row.,
                            #"currency_code_c":row.,
                            #"commission_actual_c":row.,
                            #"strike_rate_c":row.,
                            "sales_target_c":row.SalesTarget,
                            "policy_count_target_c":row.PolicyCount,
                            "currency_code_target_c":row.CurrencyCode
                            #"commission_target_c":row.,
                            #"financial_month_c":row.,
                            #"financial_year_c":row.,
                            #"name":row.
                   }
          #print(json.dumps(jsonFTrecord))
          #sys.exit()
          url = sugarcrm_generic_module.rest_url+"COV_FinancialSummary/filter"
          payload = {"filter":[{"$or":[{"account_integration_id_c":{"$equals":vfinacial_integration_id_c}}]}],"fields":"id"}
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
          sugarcrm_generic_module.checkApiCallSuccess(response,'Financial Target existence check failed.')
          if len(response.json()["records"])==0:
            print('Financial Target doesnt exist. Inserting')
            url = sugarcrm_generic_module.rest_url+"COV_FinancialSummary"
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonFTrecord)
            response = requests.request("POST", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Finacial Target creation failed for payload:'+payload)
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmFinancialTarget] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+vfinacial_integration_id_c.replace("'","''")+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('--Financial Target created successfully. vfinacial_integration_id_c:'+vfinacial_integration_id_c)
          elif len(response.json()["records"])>1:
            print('Multiple financial target records found for vfinacial_integration_id_c:'+vfinacial_integration_id_c+' Raise error')
          elif len(response.json()["records"])==1:
            print('Financial Target exists. Updating.')
            sugarUniqId=response.json()["records"][0]["id"]
            url = sugarcrm_generic_module.rest_url+"COV_FinancialSummary/"+sugarUniqId
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonFTrecord)
            response = requests.request("PUT", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Financial Target update failed for payload:'+payload)
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmFinancialTarget] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+vfinacial_integration_id_c.replace("'","''")+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('Financial Target updated successfully. vfinacial_integration_id_c:'+vfinacial_integration_id_c)
          else:
            raise;
          loop_counter=loop_counter+1
          #if loop_counter > 3: #for testing debugging only
          #  return;
       print('Total financial target records processed:'+str(loop_counter))
       print(sugarcrm_generic_module.vCountryCode+' financial target data push to SugarCRM successfully completed @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
 except Exception as excp:
    sugarcrm_generic_module.update_cursor.rollback()
    sugarcrm_generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    sugarcrm_generic_module.update_cnxn.close()
    raise Exception("Error while executing load_financial_target_data. Full error description is:"+str(excp))
#------------------------------------------------------------
#Financial actual data loading routine
def load_financialactual_data():
 try:
   if sugarcrm_generic_module.vLoadType=='Full':
     #Full Financial Actual data load block
     print('*****DISABLED******************************Deleting Financial Actual data from SugarCRM')
     #sugarcrm_generic_module.delete_module_data_using_bulk_api(sugarcrm_generic_module.access_token,"COV_FinancialSummary")
     #sys.exit()
     print('Financial Actual data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW started @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
     sugarcrm_generic_module.cursor.execute("exec dbo.rptsp_rpt0996 '"+sugarcrm_generic_module.vCountryCode+"','"+sugarcrm_generic_module.vLoadType+"','"+sugarcrm_generic_module.vDateRange+"','"+sugarcrm_generic_module.vStartDate+"','"+sugarcrm_generic_module.vEndDate+"','"+sugarcrm_generic_module.vUniqueIdentifier+"'");
     print('Financial Actual data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW completed @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
     print(sugarcrm_generic_module.vCountryCode+' Financial Actual data push to SugarCRM started @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
     jsonFARequestList=[]
     total_count=0
     loop_counter=0
     for row in sugarcrm_generic_module.cursor:
        jsonFArecord={"account_integration_id_c":row.UniqueIdentifier,
                            "policy_count_actual_c":row.PolicyCount,
                            "quote_count_actual_c":row.QuoteCount,
                            "gross_sales_actual_c":row.GrossSales,
                            "currency_code_c":row.CurrencyCode,
                            "commission_actual_c":row.Commission,
                            #"strike_rate_c":row.,
                            #"sales_target_c":row.SalesTarget,
                            #"policy_count_target_c":row.PolicyCount,
                            #"currency_code_target_c":row.CurrencyCode
                            #"commission_target_c":row.,
                            "financial_month_c":row.Month,
                            "financial_year_c":row.Month,
                            #"name":row.
                    }
        #print(jsonFArecord)
        jsonFACreateRequest={"url":"/v11_1/COV_FinancialSummary","method":"POST","data":jsonFArecord}
        #jsonFACreateRequest={"url":"/v10/COV_FinancialSummary","method":"POST","data":jsonFArecord}
        jsonFARequestList.append(jsonFACreateRequest)
        loop_counter=loop_counter+1
        sugarcrm_generic_module.update_cursor.execute("update [db-au-workspace].[dbo].[scrmFinancialSummary_UAT] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+row.UniqueIdentifier.replace("'","''")+"'")
        #if loop_counter>3:#for testing debugging only
        #     break;
        #update_cursor.commit()
        #print('row= %r' % (row,))
        if loop_counter >= 300:
         #print(jsonFARequestList)
         jsonFABulkRequest={"requests":jsonFARequestList}
         jsonFABulkRequest=json.dumps(jsonFABulkRequest)
         sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
         sugarcrm_generic_module.write_data_using_bulk_api(jsonFABulkRequest,sugarcrm_generic_module.access_token)
         sugarcrm_generic_module.update_cursor.commit()
         #print('batch written')
         total_count=total_count+loop_counter
         print('Loading batch for '+str(total_count)+' records successful.')
         loop_counter=0
         jsonFARequestList=[]
         #sys.exit()
     print('Loading remaining records')
     jsonFABulkRequest={"requests":jsonFARequestList}
     jsonFABulkRequest=json.dumps(jsonFABulkRequest)
     sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
     sugarcrm_generic_module.write_data_using_bulk_api(jsonFABulkRequest,sugarcrm_generic_module.access_token)
     sugarcrm_generic_module.update_cursor.commit()
     if loop_counter <= 300:
      total_count=total_count+loop_counter
     print('Total FA loaded:'+str(total_count))
     print(sugarcrm_generic_module.vCountryCode+' financial actual data push to SugarCRM successfully completed @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
   else:
     #Fiancial Actual delta/differential load.
       print('Financial Actual data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW started @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       sugarcrm_generic_module.cursor.execute("exec dbo.rptsp_rpt0996 '"+sugarcrm_generic_module.vCountryCode+"','"+sugarcrm_generic_module.vLoadType+"','"+sugarcrm_generic_module.vDateRange+"','"+sugarcrm_generic_module.vStartDate+"','"+sugarcrm_generic_module.vEndDate+"','"+sugarcrm_generic_module.vUniqueIdentifier+"'");
       #cursor.execute("exec dbo.rptsp_rpt0994 '"+vcountry_code+"',null,'Full'");
       #cursor.execute("exec dbo.rptsp_rpt0994 '"+'AU'+"','AU.CMA.AAN2400','Test'");
       #cursor.execute("exec dbo.etlsp_ETL099_scrmAccount '"+'AU'+"','AU.CMA.CTN0148'");
       print('Financial Actual data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW completed @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       print(sugarcrm_generic_module.vCountryCode+' Financial Actual data push to SugarCRM started @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       loop_counter=0
       
       for row in sugarcrm_generic_module.cursor:
        #####
          Aurl = sugarcrm_generic_module.rest_url+"Accounts/filter"
          Apayload = {"filter":[{"$or":[{"account_integration_id_c":{"$equals":row.UniqueIdentifier}}]}],"fields":"id"}
          Apayload = json.dumps(Apayload)
          sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
          Aheaders = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
          print("\n",Apayload,"\n")
          Aresponse = requests.request("GET", Aurl, data=Apayload, headers=Aheaders)
          #print(Aresponse.json()["records"])
          test = Aresponse.json()["records"]
          for a in test:
              AccIDValue = a["id"]
              print(AccIDValue)
         ##### 
          #continue
          #sugarcrm_generic_module.cursor.execute("select AgentName from [db-au-ods].dbo.scrmAccount where [UniqueIdentifier] = '"+str(row.UniqueIdentifier)+"'")
          #for row1 in sugarcrm_generic_module.cursor:
          #    print(row1.AgentName)
          value1 = row.UniqueIdentifier
          len_v = len(value1)
          vfinacial_actual_integration_id_c=value1[len_v-7:]
          vMonth=(datetime.datetime.strptime(row.Month,'%Y-%m-%d')).strftime('%h')
          vYear=(datetime.datetime.strptime(row.Month,'%Y-%m-%d')).strftime('%Y')
          vName=str(vfinacial_actual_integration_id_c)+"/"+str(vMonth)+"/"+str(vYear)
          #print(vName)
          jsonFArecord={"account_integration_id_c":row.UniqueIdentifier,
                            #"accounts_cov_financialsummary_1_name":"Norfolk Island Travel Centre",
                            "accounts_cov_financialsummary_1accounts_ida":str(AccIDValue),
                            "policy_count_actual_c":str(row.PolicyCount),
                            "quote_count_actual_c":str(row.QuoteCount),
                            "gross_sales_actual_c":str(float(row.GrossSales)),
                            "currency_code_c":str(row.CurrencyCode),
                            "commission_actual_c":str(float(row.Commission)),
                            #"strike_rate_c":row.,
                            #"sales_target_c":row.SalesTarget,
                            #"policy_count_target_c":row.PolicyCount,
                            #"currency_code_target_c":row.CurrencyCode
                            #"commission_target_c":row.,
                            "financial_month_c":(datetime.datetime.strptime(row.Month,'%Y-%m-%d')).strftime('%m'),
                            "financial_year_c":(datetime.datetime.strptime(row.Month,'%Y-%m-%d')).strftime('%Y'),
                            #"name":vName
                   }
          print("\n",jsonFArecord,"\n")
          #sys.exit()
          url = sugarcrm_generic_module.rest_url+"COV_FinancialSummary/filter"
          payload = {"filter":[{"$and":[{"name":{"$equals":vName}}]}],"fields":"id"}
          #payload = {"filter":[{"$and":[{"account_integration_id_c":{"$starts":vName}},{"financial_month_c":{"$equals":vMonth}},{"financial_year_c":{"$equals":vYear}}]}],"fields":"id"}
          #payload = {"filter":[{"$or":[{"account_integration_id_c":{"$equals":vfinacial_actual_integration_id_c}}]}],"fields":"id"}
          #payload = {"filter":[{"$or":[{"account_integration_id_c":{"$equals":vfinacial_actual_integration_id_c}},{"financial_month_c":{"$equals":vMonth}},{"financial_year_c":{"$equals":vYear}}]}],"fields":"id"}
          #print("\n",payload,"\n")
          payload = json.dumps(payload)
          sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
          headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
          print("\n",payload,"\n")
          response = requests.request("GET", url, data=payload, headers=headers)
          sugarcrm_generic_module.checkApiCallSuccess(response,'Financial Actual existence check failed.')
          print("\n",response.json()["records"],"\n")
          if len(response.json()["records"])==0:
            print('Financial Actual doesnt exist. Inserting')
            url = sugarcrm_generic_module.rest_url+"COV_FinancialSummary"
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonFArecord)
            response = requests.request("POST", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Finacial Actual creation failed for payload:'+payload)
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmFinancialSummary] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+vfinacial_actual_integration_id_c.replace("'","''")+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('--Financial Actual created successfully. vfinacial_actual_integration_id_c:'+vfinacial_actual_integration_id_c)
            #print(response.json()["records"])
          elif len(response.json()["records"])>1:
            print('Multiple financial actual records found for vfinacial_actual_integration_id_c:'+vfinacial_actual_integration_id_c+' Raise error')
          elif len(response.json()["records"])==1:
            print('Financial Actual exists. Updating.')
            sugarUniqId=response.json()["records"][0]["id"]
            url = sugarcrm_generic_module.rest_url+"COV_FinancialSummary/"+sugarUniqId
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonFArecord)
            response = requests.request("PUT", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Financial Actual update failed for payload:'+payload)
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmFinancialSummary] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+vfinacial_actual_integration_id_c.replace("'","''")+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('Financial Actual updated successfully. vfinacial_actual_integration_id_c:'+vfinacial_actual_integration_id_c)
          else:
            raise;
          loop_counter=loop_counter+1
          #if loop_counter > 3: #for testing debugging only
          #  return;
       print('Total financial actual records processed:'+str(loop_counter))
       print(sugarcrm_generic_module.vCountryCode+' financial actual data push to SugarCRM successfully completed @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
 except Exception as excp:
    sugarcrm_generic_module.update_cursor.rollback()
    sugarcrm_generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    sugarcrm_generic_module.update_cnxn.close()
    raise Exception("Error while executing load_financial_actual_data. Full error description is:"+str(excp))

#-------------------------------------------------------------------------------------
#Main processing block
print('***************************************************************************')
print('Main processing block started at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
sugarcrm_generic_module.validateParams()
sugarcrm_generic_module.access_token=sugarcrm_generic_module.get_token()
sugarcrm_generic_module.connect_db()

#check if there is any failed batch exist or if any active batch found. if yes, then raise error and stop. else proceed.
#All recoveries will be done manually.
#vStartTimestamp=datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')
vStartTimestamp=datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(hours=1),'%Y-%m-%d %H:00:00')
vEndTimestamp=datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(hours=1),'%Y-%m-%d %H:59:59')

#financial target data load block | Module_name is "Accounts"
#load_financialtarget_data()

#financial actual data load block | Module_name is "Accounts"
load_financialactual_data()

#Consultant Data load block | Module_name is "COV_Consultant"
#load_consultants_data()

#cursor.close()
sugarcrm_generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
sugarcrm_generic_module.update_cnxn.close()
print('Main processing block finished at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
