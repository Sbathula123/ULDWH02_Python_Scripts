#!/usr/bin/env python
# coding: utf-8

# **Details:** Code to extract the sanction list (individuals) from "https://www.dfat.gov.au/international-relations/security/sanctions/consolidated-list (link 3)" into the appropriate format required by the existing BI tables entSanctionedNames and entSanctionedDOB
# **Author:** Thahaseena KP (Capgemini)
# **Last code updated date**: 22/11/2021
# **Required files in the same folder:** regulation8_consolidated.xls **Output files:** entNames.csv, entDOB.csv, entSanctioned.csv, EntTablesNoOfRows.txt

# In[248]:


import pandas as pd
import numpy as np
colnames=['SanctionID', 'Name', 'Type', 'NameType','DOBString','COB','Citizenship','Address','Add info','Listing info','Committees','Control Date']
df = pd.read_excel(r'E:\ETL\Sanctioned\Source Files\regulation8_consolidated.csv','Sheet1',names=colnames)


# In[249]:


k = df[df['Type'].str.lower() == 'individual']


# In[250]:


k = k.drop(['Type', 'NameType','Citizenship','Add info','Listing info','Committees'], 1)


# **Get the address into a new df, so that it can be added later**

# In[251]:


#Save Address, DOBString, COB, and Conrol Date to a new df.
i = pd.DataFrame(k.SanctionID)
i['Address']=k['Address']
i['DOBString']=k['DOBString']
i['COB']=k['COB']
i['Control Date']=k['Control Date']


# In[252]:


#Remove them from k
k = k.drop(['Address'],1)
k = k.drop(['DOBString'],1)
k = k.drop(['COB'],1)
k = k.drop(['Control Date'],1)


# **Get the name into the right format - name fragments and last name**

# In[253]:


#create a new dataframe p - copy of k
p = pd.DataFrame(k.SanctionID)
p['Name'] = pd.DataFrame(k.Name)


# In[254]:


#Remove commas from Name
p["Name"] = p["Name"].str.replace(",", "")


# In[255]:


#split the names and get the last word as last name
p['NameFragment'] = p['Name'].str.rsplit(' ').str[-1] 


# In[256]:


p['LastName'] = '1'
#final Namefragment for lastnames and LastName = 1 column


# In[257]:


#create a copy to extract firstname name fragments
u=p


# In[258]:


#Remove Lastname fragments and lastName column so this can be used for Firstname fragments and lastname=0
u = u.drop(['NameFragment','LastName'],1)


# In[259]:


u['NameFragment'] =u.Name.str.rsplit(' ',1).str[0]


# In[260]:


#Get Firstname fragments
u = u.drop('NameFragment', axis=1).join(u['NameFragment'].str.split(' ', expand=True).stack().reset_index(level=1, drop=True).rename('NameFragment'))


# In[261]:


u['LastName'] = '0'
#final Namefragment for firstnames and LastName = 0 column


# **Join all name fragments and last name column from above two tables into original df k**

# In[262]:


s=k


# In[263]:


s = s.join(p.set_index('SanctionID')[['NameFragment']], on='SanctionID')


# In[264]:


s = s.join(p.set_index('SanctionID')[['LastName']], on='SanctionID')


# In[265]:


frames  = [s, u]
result = pd.concat(frames)


# In[266]:


#sort
result = result.sort_index( ascending=[True])


# In[267]:


k=result


# In[268]:


#Remove double quotes and commas from Name
k["Name"] = k["Name"].str.replace('\n', '')
k["Name"] = k["Name"].str.replace('"', "")
k["Name"] = k["Name"].str.replace(',', "")


# In[269]:


#Add in the removed columns back to k, fills them on each namefragment rows
k['Address'] = i['Address']
k['COB'] = i['COB']
k['Control Date'] = i['Control Date']


# In[270]:


#DOB


# In[271]:


k["DOBString"]=i.DOBString


# In[272]:


#Create DOB from DOBString
k['DOB']=k['DOBString']


# In[273]:


k['DOB'] = pd.to_datetime(k['DOB'],errors='coerce')
k['DOB']= k["DOB"].dt.strftime('%Y-%m-%d')


# In[274]:


#Extract MOB, YOBEnd, and YOB Start from DOB


# In[275]:


k['MOB'] = pd.DatetimeIndex(k['DOB']).month
k['YOBEnd'] = pd.DatetimeIndex(k['DOB']).year
k['YOBStart'] = k['YOBEnd']


# In[276]:


#Fill null/None values with 0
k.DOBString.fillna("null",inplace=True)
k.DOB.fillna(0,inplace=True)
k.MOB.fillna(0,inplace=True)
k.YOBEnd.fillna(0,inplace=True)
k.YOBStart.fillna(0,inplace=True)


# In[277]:


#convert from float64 to int64
k['MOB']=k['MOB'].astype(np.int64)
k['YOBStart']=k['YOBStart'].astype(np.int64)
k['YOBEnd']=k['YOBEnd'].astype(np.int64)


# In[278]:


#Fillna


# In[279]:


#Fill null/None values with 0
k.COB.fillna('null',inplace=True)
k.Address.fillna('null',inplace=True)
k['Control Date'].fillna(0,inplace=True)


# In[280]:


#remove double quotes and comma from COB
k["COB"] = k["COB"].str.replace('\n', '')
k["COB"] = k["COB"].str.replace(',', '')
k["COB"] = k["COB"].str.replace('"', '')

#Remove double quotes and commas from Address
k["Address"] = k["Address"].str.replace('\n', '')
k["Address"] = k["Address"].str.replace(',', '')
k["Address"] = k["Address"].str.replace('"', '')

#remove double quotes from COB
k["DOBString"] = k["DOBString"].str.replace('\n', '')
k["DOBString"] = k["DOBString"].str.replace(',', "")
k["DOBString"] = k["DOBString"].str.replace('"', '')

k["NameFragment"] = k["NameFragment"].str.replace('\n', '')
# In[282]:


k['COB'] = k['COB'].str[-30:]


# In[284]:


k['Country'] = 'AU'


# In[285]:


k = k[["Country","SanctionID","Name", "NameFragment", "LastName", "DOBString", "DOB", "MOB","YOBStart","YOBEnd", "COB","Address", "Control Date"]]


# **Final: Export Dataframes into csv**

# In[286]:


#entNames


# In[287]:


entNames = k[['Country','SanctionID','Name','NameFragment','LastName','COB','Address']]


# In[288]:


import csv
entNames.to_csv(r'E:\ETL\Sanctioned\Converted Files\entNames_Link3.csv', index=False)
#,quoting=csv.QUOTE_NONE


# In[289]:


#entDOB


# In[290]:


entDOB = k[['Country','SanctionID','DOBString','DOB','MOB','YOBStart','YOBEnd']]


# In[291]:


entDOB = entDOB[~entDOB.index.duplicated(keep='first')]


# In[292]:


entDOB.to_csv(r'E:\ETL\Sanctioned\Converted Files\entDOB_Link3.csv', index=False)


# In[293]:


#entSanctioned


# In[294]:


entSanctioned = k[['Country','SanctionID','Name','DOBString',"Control Date"]]


# In[295]:


entSanctioned = entSanctioned[~entSanctioned.index.duplicated(keep='first')]


# In[296]:


entSanctioned.to_csv(r'E:\ETL\Sanctioned\Converted Files\entSanctioned_Link3.csv', index=False)


# In[297]:


#Logfile


# In[298]:


#entNames
entNamesindex = entNames.index
number_of_rowsNames = len(entNamesindex)


# In[299]:


#entNames
entDOBindex = entDOB.index
number_of_rowsDOB = len(entDOBindex)


# In[300]:


#entDOB
entDOBindex = entDOB.index
number_of_rowsSanctioned = len(entDOBindex)


# In[301]:


from datetime import datetime
now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")


# In[302]:


c="Date and time: " + dt_string + " Total No Of Rows:- entNames: " + str(number_of_rowsNames) + " entDOB: " + str(number_of_rowsDOB), "entSanctioned: " + str(number_of_rowsSanctioned)
c =str(c)


# In[303]:


file = open("EntTablesNoOfRows.txt", "w") 
file.write(c)
file.close() 

