#!/usr/bin/env python
# coding: utf-8

# **Details:** Code to extract the sanction list (individuals) from "https://www.police.govt.nz/advice/personal-community/counterterrorism/designated-entities/lists-associated-with-resolutions-1267-1989-2253-1988; (link 5.1 NZ)" into the appropriate format required by the existing BI tables entSanctionedNames and entSanctionedDOB
# **Author:** Thahaseena KP (Capgemini)
# **Last code updated date**: 01/12/2021
# **Required files in the same folder:** designated-entities-7-september-2021.xlsx (with updated date) **Output files:** entNames.csv, entDOB.csv, entSanctioned.csv, EntTablesNoOfRows.txt

# In[ ]:


import pandas as pd
import numpy as np
df = pd.read_excel('designated-entities-7-september-2021.xlsx','Individuals')


# In[ ]:


k = pd.DataFrame(df.Sanctions_List_Permanent_Reference_Number)


# In[ ]:


k[['Reference','FullLName','FullFName_1','DOBString','COB','Address']] = df[['ID','LastName', 'Name_1', 'DOB_1', 'POB_1_Country', 'Address']]


# In[ ]:


#Combine first and last names
k['Name'] = k[['FullFName_1', 'FullLName']].apply(lambda x: ' '.join(x.dropna()), axis=1)


# In[ ]:


#Remove seperate first and last names
k = k.drop(['FullLName'],1)
k = k.drop(['FullFName_1'],1)


# In[ ]:


k.rename(columns = {'Sanctions_List_Permanent_Reference_Number':'SanctionID'}, inplace = True)


# **Get the columns into a new df, so that it can be added after NameFragments**

# In[ ]:


#Save Address and DOBString to a new df columns
i = pd.DataFrame(k.SanctionID)
i['Address']=k['Address']
i['DOBString']=k['DOBString']
i['COB']=k['COB']


# In[ ]:


#Remove Address and DOBString from k
k = k.drop(['Address'],1)
k = k.drop(['DOBString'],1)
k = k.drop(['COB'],1)


# **Get the name into the right format - name fragments and last name**

# In[ ]:


#Remove double quotes, commas, incorrect values from Name
k["Name"] = k["Name"].str.replace(',', '')
k["Name"] = k["Name"].str.replace('"', '')
k["Name"] = k["Name"].str.replace('(previously listed as)', '')


# In[ ]:


p = pd.DataFrame(k.Reference)
p['SanctionID'] = pd.DataFrame(k.SanctionID)
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


s = s.join(p.set_index('Reference')[['NameFragment']], on='Reference')


# In[ ]:


s = s.join(p.set_index('Reference')[['LastName']], on='Reference')


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
k.DOB.fillna(0,inplace=True)
k.MOB.fillna(0,inplace=True)
k.YOBEnd.fillna(0,inplace=True)
k.YOBStart.fillna(0,inplace=True)


# In[ ]:


#convert from float64 to int64
k['MOB']=k['MOB'].astype(np.int64)
k['YOBStart']=k['YOBStart'].astype(np.int64)
k['YOBEnd']=k['YOBEnd'].astype(np.int64)


# **Fill null/None values of string type with null**

# In[ ]:


k.DOBString.fillna('null',inplace=True)
k.COB.fillna('null',inplace=True)
k.Address.fillna('null',inplace=True)


# **Remove double quotes, comma, and line breaks**

# In[ ]:


k["Name"] = k["Name"].str.replace('\n', '')
k["Name"] = k["Name"].str.replace(',', '')
k["Name"] = k["Name"].str.replace('"', '')

k["NameFragment"] = k["NameFragment"].str.replace('\n', '')
k["NameFragment"] = k["NameFragment"].str.replace(',', '')
k["NameFragment"] = k["NameFragment"].str.replace('"', '')

k["DOBString"] = k["DOBString"].str.replace('\n', '')
k["DOBString"] = k["DOBString"].str.replace(',', '')
k["DOBString"] = k["DOBString"].str.replace('"', '')

k["COB"] = k["COB"].str.replace('\n', '')
k["COB"] = k["COB"].str.replace(',', '')
k["COB"] = k["COB"].str.replace('"', '')

k["Address"] = k["Address"].str.replace('\n', '')
k["Address"] = k["Address"].str.replace(',', '|')
k["Address"] = k["Address"].str.replace('"', '')


# **Limit the characters to 30 for COB**

# In[ ]:


k['COB'] = k['COB'].str[-30:]


# **Add Country column**

# In[ ]:


k['Country'] = 'NZ'


# **Swap the columns**

# In[ ]:


k = k[["Country","SanctionID","Reference", "Name", "NameFragment", "LastName", "DOBString", "DOB", "MOB","YOBStart","YOBEnd", "COB","Address"]]


# **Final: Export Dataframes into csv**

# In[ ]:


#entNames


# In[ ]:


entNames = k[['Country','SanctionID','Reference','Name','NameFragment','LastName','COB','Address']]


# In[ ]:


entNames.to_csv('entNames.csv', index=False)


# In[ ]:


#entDOB


# In[ ]:


entDOB = k[['Country','SanctionID','Reference','DOBString','DOB','MOB','YOBStart','YOBEnd']]


# In[ ]:


entDOB = entDOB[~entDOB.index.duplicated(keep='first')]


# In[ ]:


entDOB.to_csv('entDOB.csv', index=False)


# In[ ]:


#entSanctioned


# In[ ]:


entSanctioned = k[['Country','SanctionID','Reference', 'Name','DOBString']]


# In[ ]:


entSanctioned = entSanctioned[~entSanctioned.index.duplicated(keep='first')]


# In[ ]:


entSanctioned.to_csv('entSanctioned.csv', index=False)


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


#Open a file with access mode 'a'
with open("EntTablesNoOfRows.txt", "a",newline='\n') as file_object:
    file_object.write(c+"\n")

