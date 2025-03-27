#!/usr/bin/env python
# coding: utf-8

# **Details:** Code to extract the sanction list (individuals) from "https://www.gov.uk/government/publications/the-uk-sanctions-list; (link 2 UK)" into the appropriate format required by the existing BI tables entSanctionedNames and entSanctionedDOB
# **Author:** Thahaseena KP (Capgemini)
# **Last code updated date**: 26/11/2021
# **Required files in the same folder:** uk-sanctions-list.xlsx **Output files:** entNames.csv, entDOB.csv, entSanctioned.csv, EntTablesNoOfRows.txt

# In[ ]:


import pandas as pd
import numpy as np
df = pd.read_excel(r'E:\ETL\Sanctioned\Source Files\uk-sanctions-list.xls','Individual')


# In[ ]:


df = df[~df['OFSI ID'].duplicated(keep='first')]


# In[ ]:


k = pd.DataFrame(df['OFSI ID'])


# In[ ]:


k[['Name','DOBString','COB']] = df[['Primary Name','D.O.B','Country of birth']]


# In[ ]:


l = pd.DataFrame(df['OFSI ID'])


# In[ ]:


l[['Address Line 1','Address Line 2','Address Line 3','Address Line 4', 'Postcode', 'Primary Address Country']] = df[['Address Line 1','Address Line 2','Address Line 3','Address Line 4', 'Postcode', 'Primary Address Country']]


# In[ ]:


#Combine Address
k['Address'] = l[['Address Line 1','Address Line 2','Address Line 3','Address Line 4', 'Postcode', 'Primary Address Country']].apply(lambda x: '|'.join(x.dropna()), axis=1)


# In[ ]:


k["Address"] = k["Address"].str.replace('Unknown', '')
k["Address"] = k["Address"].str.replace('unknown', '')
k["Address"] = k["Address"].str.replace('|', '')


# In[ ]:


k = k.replace(r'^\s*$', np.nan, regex=True)


# In[ ]:


#Replace unknown values as null
k["COB"] = k["COB"].str.title().replace('Unknown', 'null')
k["DOBString"] = k["DOBString"].str.title().replace('Unknown', 'null')


# In[ ]:


#Remove double quotes and commas from Name
k["Name"] = k["Name"].str.replace('"', "")
k["Name"] = k["Name"].str.replace(',', "")


# In[ ]:


k.rename(columns = {'OFSI ID':'SanctionID'}, inplace = True)


# **Get the columns into a new df, so that it can be added after NameFragments**

# In[ ]:


#Save Address, DOBString & COB to a new df columns
i = pd.DataFrame(k.SanctionID)
i['Address']=k['Address']
i['DOBString']=k['DOBString']
i['COB']=k['COB']


# In[ ]:


#Remove Address, DOBString & COB from k
k = k.drop(['Address'],1)
k = k.drop(['DOBString'],1)
k = k.drop(['COB'],1)


# **Get the name into the right format - name fragments and last name**

# In[ ]:


#Name


# In[ ]:


p = pd.DataFrame(k.SanctionID)
p['Name'] = pd.DataFrame(k.Name)


# In[ ]:


#split the names and get the last word as last name
p['NameFragment'] = p['Name'].str.rsplit(' ').str[-1] 


# In[ ]:


p['LastName'] = '1'
#final Namefragment for lastnames and LastName = 1 column


# In[ ]:


#create a copy to extract firstname name fragments
u=p


# In[ ]:


#Remove Lastname fragments and lastName column so this can be used for Firstname fragments and lastname=0
u = u.drop(['NameFragment','LastName'],1)


# In[ ]:


#Remove single word names as they are considered as lastnames
u = u[u.Name.str.count(' ') > 0]


# In[ ]:


#Split the names (without last word)
u['NameFragment'] =u.Name.str.rsplit(' ',1).str[0]


# In[ ]:


#Get Firstname fragments
u = u.drop('NameFragment', axis=1).join(u['NameFragment'].str.split(' ', expand=True).stack().reset_index(level=1, drop=True).rename('NameFragment'))


# In[ ]:


u['LastName'] = '0'
#final Namefragment for firstnames and LastName = 0 column


# **Join all name fragments and last name column from above two tables into original df k**

# In[ ]:


s=k


# In[ ]:


s = s.join(p.set_index('SanctionID')[['NameFragment']], on='SanctionID')


# In[ ]:


s = s.join(p.set_index('SanctionID')[['LastName']], on='SanctionID')


# In[ ]:


frames  = [s, u]
result = pd.concat(frames)


# In[ ]:


#sort
result = result.sort_index( ascending=[True])


# In[ ]:


k=result


# In[ ]:


#Add in the removed columns back to k, fills them on each namefragment rows
k['Address'] = i['Address']
k['COB'] = i['COB']
k['DOBString'] = i['DOBString']


# In[ ]:


#DOB


# In[ ]:


#Create DOB from DOBString
k['DOB']=k['DOBString']


# In[ ]:


k['DOB'] = pd.to_datetime(k['DOB'],errors='coerce')
k['DOB']= k["DOB"].dt.strftime('%Y-%m-%d')


# In[ ]:


#Extract MOB, YOBEnd, and YOB Start from DOB
k['MOB'] = pd.DatetimeIndex(k['DOB']).month
k['YOBEnd'] = pd.DatetimeIndex(k['DOB']).year
k['YOBStart'] = k['YOBEnd']


# In[ ]:


#Fill null/None values with 0
k.DOBString.fillna("null",inplace=True)
k.DOB.fillna(0,inplace=True)
k.MOB.fillna(0,inplace=True)
k.YOBEnd.fillna(0,inplace=True)
k.YOBStart.fillna(0,inplace=True)
k.COB.fillna("null",inplace=True)
k.Address.fillna("null",inplace=True)


# In[ ]:


#convert from float64 to int64
k['MOB']=k['MOB'].astype(np.int64)
k['YOBStart']=k['YOBStart'].astype(np.int64)
k['YOBEnd']=k['YOBEnd'].astype(np.int64)


# In[ ]:


#replace double quotes and commas from COB
k["COB"] = k["COB"].str.replace(',', '')
k["COB"] = k["COB"].str.replace('"', '')

#Remove double quotes and commas from Address
k["Address"] = k["Address"].str.replace('\n', '')
k["Address"] = k["Address"].str.replace(',', '')
k["Address"] = k["Address"].str.replace('"', '')

#remove double quotes and commas from DOBString
k["DOBString"] = k["DOBString"].str.replace('\n', "")
k["DOBString"] = k["DOBString"].str.replace(',', "")
k["DOBString"] = k["DOBString"].str.replace('"', '')


# In[ ]:


k['Country'] = 'UK'


# In[ ]:


k = k[["Country","SanctionID","Name", "NameFragment", "LastName", "DOBString", "DOB", "MOB","YOBStart","YOBEnd", "COB","Address"]]


# **Final: Export Dataframes into csv**

# In[ ]:


#entNames


# In[ ]:


entNames = k[['Country','SanctionID','Name','NameFragment','LastName','COB','Address']]


# In[ ]:


entNames.to_csv(r'E:\ETL\Sanctioned\Converted Files\entNames_Link2.csv', index=False)


# In[ ]:


#entDOB


# In[ ]:


entDOB = k[['Country','SanctionID','DOBString','DOB','MOB','YOBStart','YOBEnd']]


# In[ ]:


entDOB = entDOB[~entDOB.index.duplicated(keep='first')]


# In[ ]:


entDOB.to_csv(r'E:\ETL\Sanctioned\Converted Files\entDOB_Link2.csv', index=False)


# In[ ]:


#entSanctioned


# In[ ]:


entSanctioned = k[['Country','SanctionID','Name','DOBString']]


# In[ ]:


entSanctioned = entSanctioned[~entSanctioned.index.duplicated(keep='first')]


# In[ ]:


entSanctioned.to_csv(r'E:\ETL\Sanctioned\Converted Files\entSanctioned_Link2.csv', index=False)


# In[ ]:


#Logfile


# In[ ]:


#entNames
entNamesindex = entNames.index
number_of_rowsNames = len(entNamesindex)


# In[ ]:


#entNames
entDOBindex = entDOB.index
number_of_rowsDOB = len(entDOBindex)


# In[ ]:


#entDOB
entDOBindex = entDOB.index
number_of_rowsSanctioned = len(entDOBindex)


# In[ ]:


from datetime import datetime
now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")


# In[ ]:


c="Date and time: " + dt_string + " Total No Of Rows:- entNames: " + str(number_of_rowsNames) + " entDOB: " + str(number_of_rowsDOB), "entSanctioned: " + str(number_of_rowsSanctioned)
c =str(c)


# In[ ]:


file = open("EntTablesNoOfRows.txt", "w") 
file.write(c)
file.close() 

