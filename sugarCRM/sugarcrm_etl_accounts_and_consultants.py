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
#Accounts data loading routine
def load_accounts_data():
 try:
   if sugarcrm_generic_module.vLoadType=='Full':
     #Full accounts data load block
     print('*****DISABLED******************************Deleting Account data from SugarCRM')
     #sugarcrm_generic_module.delete_module_data_using_bulk_api(sugarcrm_generic_module.access_token,"Accounts")
     #sys.exit()
     print('Account data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW started @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
     sugarcrm_generic_module.cursor.execute("exec dbo.rptsp_rpt0994 '"+sugarcrm_generic_module.vCountryCode+"','"+sugarcrm_generic_module.vLoadType+"','"+sugarcrm_generic_module.vDateRange+"','"+sugarcrm_generic_module.vStartDate+"','"+sugarcrm_generic_module.vEndDate+"','"+sugarcrm_generic_module.vUniqueIdentifier+"'");
     print('Account data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW completed @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
     print(sugarcrm_generic_module.vCountryCode+' account data push to SugarCRM started @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
     jsonAccountRequestList=[]
     total_count=0
     loop_counter=0
     for row in sugarcrm_generic_module.cursor:
        jsonAccountrecord={"account_integration_id_c":row.UniqueIdentifier,
                           "domain_code_c":row.DomainCode,
                           "company_code_c":row.CompanyCode,
                            "group_code_c":row.GroupCode,
                            "sub_group_code_c":row.SubGroupCode,
                            "group_name_c":row.GroupName,
                            "sub_group_name_c":row.SubGroupName,
                            "name":row.AgentName,
                            "alpha_code_c":row.AgencyCode,
                            "trading_status_c":row.Status,
                            "branch_c":row.Branch,
                            "shipping_address_street":row.BillingStreet,
                            "shipping_address_city":row.BillingSuburb,
                            "shipping_address_state":row.BillingState,
                            "shipping_address_postalcode":row.BillingPostCode,
                            "shipping_address_country":row.BillingCountry,
                            "phone_office":row.OfficePhone,
                            "email1":row.EmailAddress,
                            "billing_address_street":row.ShippingStreet,
                            "billing_address_city":row.ShippingSuburb,
                            "billing_address_state":row.ShippingState,
                            "billing_address_postalcode":row.ShippingPostCode,
                            "billing_address_country":row.ShippingCountry,
                            #"Assignedto":row.BDM,
                            "bdm_c":row.BDM,
                            "ownership":row.AccountManager,
                            "bdm_call_frequency_c":row.BDMCallFrequency,
                            "agent_call_frequency_c":row.AccountCallFrequency,
                            "sales_tier_c":row.SalesTier,
                            "outlet_type_c":row.OutletType,
                            "flight_centre_area_c":row.FCArea,
                            "flight_centre_nation_c":row.FCNation,
                            "flight_centre_egm_nation_c":row.EGMNation,
                            "agency_grading_c":row.AgencyGrading,
                            "salutation_c":row.Title,
                            "agency_first_name_c":row.FirstName,
                            "agency_last_name_c":row.LastName,
                            "agency_email_c":row.ManagerEmail,
                            "credit_card_sales_only_c":row.CCSaleOnly,
                            "payment_type_c":row.PaymentType,
                            "account_email_c":row.AccountEmail,
                            "sales_segment_c":row.SalesSegment,
                            "related_outlet_c":row.PreviousUniqueIdentifier,
                            "account_type":row.AccountType
                            #"quadrant_c":row.
                            #"annual_ttv_c":row.
                            #"total_insurance_sales_c":row.
                            #"last_visit_c":row.
                            #"last_activity_c":row.
                            #"LBL_DATE_ENTERED":row.
                            #"LBL_DATE_MODIFIED":row.
                            #"LBL_DESCRIPTION":row.
                   }
        #print(jsonAccountrecord)
        jsonAccountCreateRequest={"url":"/v11_1/Accounts","method":"POST","data":jsonAccountrecord}
        jsonAccountRequestList.append(jsonAccountCreateRequest)
        loop_counter=loop_counter+1
        sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmAccount] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+row.UniqueIdentifier.replace("'","''")+"'")
        #if loop_counter>3:#for testing debugging only
        #     break;
        #update_cursor.commit()
        #print('row= %r' % (row,))
        if loop_counter >= 300:
         #print(jsonAccountRequestList)
         jsonAccountBulkRequest={"requests":jsonAccountRequestList}
         jsonAccountBulkRequest=json.dumps(jsonAccountBulkRequest)
         sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
         sugarcrm_generic_module.write_data_using_bulk_api(jsonAccountBulkRequest,sugarcrm_generic_module.access_token)
         sugarcrm_generic_module.update_cursor.commit()
         #print('batch written')
         total_count=total_count+loop_counter
         print('Loading batch for '+str(total_count)+' records successful.')
         loop_counter=0
         jsonAccountRequestList=[]
         #sys.exit()
     print('Loading remaining records')
     jsonAccountBulkRequest={"requests":jsonAccountRequestList}
     jsonAccountBulkRequest=json.dumps(jsonAccountBulkRequest)
     sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
     sugarcrm_generic_module.write_data_using_bulk_api(jsonAccountBulkRequest,sugarcrm_generic_module.access_token)
     sugarcrm_generic_module.update_cursor.commit()
     if loop_counter <= 300:
      total_count=total_count+loop_counter
     print('Total accounts loaded:'+str(total_count))
     print(sugarcrm_generic_module.vCountryCode+' account data push to SugarCRM successfully completed @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
   else:
     #Accounts delta/differential load.
       print('Account data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW started @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       sugarcrm_generic_module.cursor.execute("exec dbo.rptsp_rpt0994 '"+sugarcrm_generic_module.vCountryCode+"','"+sugarcrm_generic_module.vLoadType+"','"+sugarcrm_generic_module.vDateRange+"','"+sugarcrm_generic_module.vStartDate+"','"+sugarcrm_generic_module.vEndDate+"','"+sugarcrm_generic_module.vUniqueIdentifier+"'");
       #cursor.execute("exec dbo.rptsp_rpt0994 '"+vcountry_code+"',null,'Full'");
       #cursor.execute("exec dbo.rptsp_rpt0994 '"+'AU'+"','AU.CMA.AAN2400','Test'");
       #cursor.execute("exec dbo.etlsp_ETL099_scrmAccount '"+'AU'+"','AU.CMA.CTN0148'");
       print('Account data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW completed @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       print(sugarcrm_generic_module.vCountryCode+' account data push to SugarCRM started @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       loop_counter=0
       for row in sugarcrm_generic_module.cursor:
          #continue
          vaccount_integration_id_c=row.UniqueIdentifier
          jsonAccountrecord={"account_integration_id_c":row.UniqueIdentifier,
                           "domain_code_c":row.DomainCode,
                           "company_code_c":row.CompanyCode,
                            "group_code_c":row.GroupCode,
                            "sub_group_code_c":row.SubGroupCode,
                            "group_name_c":row.GroupName,
                            "sub_group_name_c":row.SubGroupName,
                            "name":row.AgentName,
                            "alpha_code_c":row.AgencyCode,
                            "trading_status_c":row.Status,
                            "branch_c":row.Branch,
                            "shipping_address_street":row.BillingStreet,
                            "shipping_address_city":row.BillingSuburb,
                            "shipping_address_state":row.BillingState,
                            "shipping_address_postalcode":row.BillingPostCode,
                            "shipping_address_country":row.BillingCountry,
                            "phone_office":row.OfficePhone,
                            "email1":row.EmailAddress,
                            "billing_address_street":row.ShippingStreet,
                            "billing_address_city":row.ShippingSuburb,
                            "billing_address_state":row.ShippingState,
                            "billing_address_postalcode":row.ShippingPostCode,
                            "billing_address_country":row.ShippingCountry,
                            #"Assignedto":row.BDM,
                            "bdm_c":row.BDM,
                            "ownership":row.AccountManager,
                            "bdm_call_frequency_c":row.BDMCallFrequency,
                            "agent_call_frequency_c":row.AccountCallFrequency,
                            "sales_tier_c":row.SalesTier,
                            "outlet_type_c":row.OutletType,
                            "flight_centre_area_c":row.FCArea,
                            "flight_centre_nation_c":row.FCNation,
                            "flight_centre_egm_nation_c":row.EGMNation,
                            "agency_grading_c":row.AgencyGrading,
                            "salutation_c":row.Title,
                            "agency_first_name_c":row.FirstName,
                            "agency_last_name_c":row.LastName,
                            "agency_email_c":row.ManagerEmail,
                            "credit_card_sales_only_c":row.CCSaleOnly,
                            "payment_type_c":row.PaymentType,
                            "account_email_c":row.AccountEmail,
                            "sales_segment_c":row.SalesSegment,
                            "related_outlet_c":row.PreviousUniqueIdentifier,
                            "account_type":row.AccountType
                            #"quadrant_c":row.
                            #"annual_ttv_c":row.
                            #"total_insurance_sales_c":row.
                            #"last_visit_c":row.
                            #"last_activity_c":row.
                            #"LBL_DATE_ENTERED":row.
                            #"LBL_DATE_MODIFIED":row.
                            #"LBL_DESCRIPTION":row.
                   }
          #print(json.dumps(jsonAccountrecord))
          #sys.exit()
          url = sugarcrm_generic_module.rest_url+"Accounts/filter"
          payload = {"filter":[{"$or":[{"account_integration_id_c":{"$equals":vaccount_integration_id_c}}]}],"fields":"id"}
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
          sugarcrm_generic_module.checkApiCallSuccess(response,'Account existence check failed.')
          if len(response.json()["records"])==0:
            print('Account doesnt exist. Inserting')
            url = sugarcrm_generic_module.rest_url+"Accounts"
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonAccountrecord)
            response = requests.request("POST", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Account creation failed for payload:'+payload)
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmAccount] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+vaccount_integration_id_c.replace("'","''")+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('--Account created successfully. vaccount_integration_id_c:'+vaccount_integration_id_c)
          elif len(response.json()["records"])>1:
            print('Multiple account records found for vaccount_integration_id_c:'+vaccount_integration_id_c+' Raise error')
          elif len(response.json()["records"])==1:
            print('Account exists. Updating.')
            sugarUniqId=response.json()["records"][0]["id"]
            url = sugarcrm_generic_module.rest_url+"Accounts/"+sugarUniqId
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonAccountrecord)
            response = requests.request("PUT", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Account update failed for payload:'+payload)
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmAccount] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+vaccount_integration_id_c.replace("'","''")+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('Account updated successfully. vaccount_integration_id_c:'+vaccount_integration_id_c)
          else:
            raise;
          loop_counter=loop_counter+1
          #if loop_counter > 3: #for testing debugging only
          #  return;
       print('Total account records processed:'+str(loop_counter))
       print(sugarcrm_generic_module.vCountryCode+' account data push to SugarCRM successfully completed @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
 except Exception as excp:
    sugarcrm_generic_module.update_cursor.rollback()
    sugarcrm_generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    sugarcrm_generic_module.update_cnxn.close()
    raise Exception("Error while executing load_accounts_data. Full error description is:"+str(excp))
#------------------------------------------------------------
#Consultant data loading routine
def load_consultants_data():
 try:
   if sugarcrm_generic_module.vLoadType=='Full':
       print('*****DISABLED******************************Deleting Consultant data from SugarCRM')
       #sugarcrm_generic_module.delete_module_data_using_bulk_api(sugarcrm_generic_module.access_token,"COV_Consultant")
       print('Consultant Data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW started @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       #cursor.execute("exec dbo.etlsp_ETL099_scrmConsultant '"+vcountry_code+"',null");
       #cursor.execute("exec dbo.rptsp_rpt0995 '"+'AU'+"','AU.CMA.AAN2400.covermore','Test'");
       #print("test printing exec dbo.rptsp_rpt0995 '"+vCountryCode+"','"+vLoadType+"','"+vDateRange+"','"+vStartDate+"','"+vEndDate+"','"+vUniqueIdentifier+"'");
       sugarcrm_generic_module.cursor.execute("exec dbo.rptsp_rpt0995 '"+sugarcrm_generic_module.vCountryCode+"','"+sugarcrm_generic_module.vLoadType+"','"+sugarcrm_generic_module.vDateRange+"','"+sugarcrm_generic_module.vStartDate+"','"+sugarcrm_generic_module.vEndDate+"','"+sugarcrm_generic_module.vUniqueIdentifier+"'");
       print('Consultant Data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW completed @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       print(sugarcrm_generic_module.vCountryCode +' Consultant data push to SugarCRM started @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       jsonConsultantRequestList=[]
       total_count=0
       loop_counter=0
       for row in sugarcrm_generic_module.cursor:
          #continue
          jsonConsultantrecord={"consultant_id_c":row.UniqueIdentifier,
                                #"name":row.Name,
                                "first_name_c":row.FirstName,
                                "last_name_c":row.LastName,
                                "user_name_c":row.UserName,
                                #"user_type_c":row.UserType,
                                "user_type_c":row.UserType.replace('(','').replace(')',''),#As requested by Poornima. Looks like there is another logic at API level to then reconvert and reintroduce paranthesis in the API layer when displaying it on screen.
                                "account_integration_id_c":row.OutletUniqueIdentifier,
                                "status_c":row.Status,
                                "inactive_status_date_c":row.InactiveStatusDate,
                                "email_c":row.Email,
                                "dob_c":row.DateOfBirth
                                #"description":row.
                     }
          #print(jsonAccountrecord)
          jsonConsultantCreateRequest={"url":"/v11_1/COV_Consultant","method":"POST","data":jsonConsultantrecord}
          jsonConsultantRequestList.append(jsonConsultantCreateRequest)
          loop_counter=loop_counter+1
          #print("update [db-au-ods].[dbo].[scrmConsultant] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+row.UniqueIdentifier+"'")
          sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmConsultant] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+row.UniqueIdentifier.replace("'","''")+"'")
          #if loop_counter>3:#for testing debugging only
          #   break;
          #print('row= %r' % (row,))
          if loop_counter >= 300:
           #print(jsonAccountRequestList)
           sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
           jsonConsultantBulkRequest={"requests":jsonConsultantRequestList}
           jsonConsultantBulkRequest=json.dumps(jsonConsultantBulkRequest)
           sugarcrm_generic_module.write_data_using_bulk_api(jsonConsultantBulkRequest,sugarcrm_generic_module.access_token)
           sugarcrm_generic_module.update_cursor.commit()
           #print('batch written')
           total_count=total_count+loop_counter
           print('Loading batch for '+str(total_count)+' records successful.')
           loop_counter=0
           jsonConsultantRequestList=[]
           #sys.exit()
       print('Loading remaining records')     
       jsonConsultantBulkRequest={"requests":jsonConsultantRequestList}
       jsonConsultantBulkRequest=json.dumps(jsonConsultantBulkRequest)
       sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
       sugarcrm_generic_module.write_data_using_bulk_api(jsonConsultantBulkRequest,sugarcrm_generic_module.access_token)
       sugarcrm_generic_module.update_cursor.commit()
       if loop_counter <= 300:
        total_count=total_count+loop_counter
       print('Total records loaded:'+str(total_count))
       print(sugarcrm_generic_module.vCountryCode+' Consultant data push to SugarCRM successfully completed @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
   else:
       #Code for delta/differential load.
       print('Consultant Data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW started @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       #cursor.execute("exec dbo.etlsp_ETL099_scrmConsultant '"+vcountry_code+"',null");
       #cursor.execute("exec dbo.rptsp_rpt0995 '"+'AU'+"','AU.CMA.AAN2400.covermore','Test'");
       #print("test printing exec dbo.rptsp_rpt0995 '"+vCountryCode+"','"+vLoadType+"','"+vDateRange+"','"+vStartDate+"','"+vEndDate+"','"+vUniqueIdentifier+"'");
       sugarcrm_generic_module.cursor.execute("exec dbo.rptsp_rpt0995 '"+sugarcrm_generic_module.vCountryCode+"','"+sugarcrm_generic_module.vLoadType+"','"+sugarcrm_generic_module.vDateRange+"','"+sugarcrm_generic_module.vStartDate+"','"+sugarcrm_generic_module.vEndDate+"','"+sugarcrm_generic_module.vUniqueIdentifier+"'");
       print('Consultant Data fetch for '+sugarcrm_generic_module.vCountryCode+' from EDW completed @'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       print(sugarcrm_generic_module.vCountryCode +' Consultant data push to SugarCRM started @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
       #jsonConsultantRequestList=[]
       #total_count=0
       loop_counter=0
       for row in sugarcrm_generic_module.cursor:
          #continue
          vconsultant_id_c=row.UniqueIdentifier
          jsonConsultantrecord={"consultant_id_c":row.UniqueIdentifier,
                                #"name":row.Name,
                                "first_name_c":row.FirstName,
                                "last_name_c":row.LastName,
                                "user_name_c":row.UserName,
                                "user_type_c":row.UserType.replace('(','').replace(')',''),
                                "account_integration_id_c":row.OutletUniqueIdentifier,
                                "status_c":row.Status,
                                "inactive_status_date_c":row.InactiveStatusDate,
                                "email_c":row.Email,
                                "dob_c":row.DateOfBirth
                                #"description":row.
                     }
          #print(jsonAccountrecord)
          url = sugarcrm_generic_module.rest_url+"COV_Consultant/filter"
          payload = {"filter":[{"$or":[{"consultant_id_c":{"$equals":vconsultant_id_c}}]}],"fields":"id"}
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
          sugarcrm_generic_module.checkApiCallSuccess(response,'Consultant existence check failed.')
          if len(response.json()["records"])==0:
            print('Consultant doesnt exist. Inserting')
            url = sugarcrm_generic_module.rest_url+"COV_Consultant"
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonConsultantrecord)
            response = requests.request("POST", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Consultant creation failed for payload:'+payload)
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmConsultant] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+vconsultant_id_c.replace("'","''")+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('--Consultant created successfully. vconsultant_id_c:'+vconsultant_id_c)
          elif len(response.json()["records"])>1:
            print('Multiple Consultant records found. Raise error')
          elif len(response.json()["records"])==1:
            print('Consultant exists. Updating.')
            sugarUniqId=response.json()["records"][0]["id"]
            url = sugarcrm_generic_module.rest_url+"COV_Consultant/"+sugarUniqId
            sugarcrm_generic_module.access_token=sugarcrm_generic_module.validate_token(sugarcrm_generic_module.access_token)
            headers = {
              'OAuth-Token': sugarcrm_generic_module.access_token,
              'Content-Type': "application/json",
              'Cache-Control': "no-cache",
              'Postman-Token': "78ecc39f-9ea7-43ee-a689-587b5d8ef5b6"
              }
            payload=json.dumps(jsonConsultantrecord)
            response = requests.request("PUT", url, data=payload, headers=headers)
            sugarcrm_generic_module.checkApiCallSuccess(response,'Consultant  update failed for payload:'+payload)
            print("update [db-au-ods].[dbo].[scrmConsultant] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+vconsultant_id_c.replace("'","''")+"'")
            sugarcrm_generic_module.update_cursor.execute("update [db-au-ods].[dbo].[scrmConsultant] set isSynced='Y',SyncedDateTime=getdate() where uniqueIdentifier='"+vconsultant_id_c.replace("'","''")+"'")
            sugarcrm_generic_module.update_cursor.commit()
            print('Consultant updated successfully.vconsultant_id_c:'+vconsultant_id_c)
          else:
            raise;
          loop_counter=loop_counter+1
          #if loop_counter > 3:#for testing debugging only
          #  break;
       print('Total consultant records processed:'+str(loop_counter))
       print(sugarcrm_generic_module.vCountryCode+' Consultant data push to SugarCRM successfully completed @:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
 except Exception as excp:
    sugarcrm_generic_module.update_cursor.rollback()
    sugarcrm_generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
    sugarcrm_generic_module.update_cnxn.close()
    raise Exception("Error while executing load_consultants_data. Full error description is:"+str(excp))

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

#account data load block | Module_name is "Accounts"
load_accounts_data()

#Consultant Data load block | Module_name is "COV_Consultant"
load_consultants_data()

#cursor.close()
sugarcrm_generic_module.cnxn.close()#closing cursor, doesnt clear lock. Closing connection is important to release the lock.
sugarcrm_generic_module.update_cnxn.close()
print('Main processing block finished at:'+str(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))