#!/usr/bin/env python
# coding: utf-8

# **Details:** Code to extract the sanction list (individuals) from "https://sanctionssearch.ofac.treas.gov/ (link 1 US)" into the appropriate format required by the existing BI tables entSanctionedNames and entSanctionedDOB
# **Author:** Thahaseena KP (Capgemini)
# **Last code updated date**: 01/12/2021
# **Required files in the same folder:** sdn.csv and add.csv **Output files:** entNames.csv, entDOB.csv, entSanctioned.csv, EntTablesNoOfRows.txt

# **Load the sanction list/data and give column names**

# In[ ]:


import pandas as pd
import numpy as np
colnames=['SanctionID', 'Name', 'isIndividual', 'col4','col5','col6','col7','col8','col9','col10','col11','details']
df = pd.read_csv('sdn.csv',sep=',', header=None, names=colnames)


# **Remove organisation records and only keep the list of individuals**

# In[ ]:


k = df.loc[df['isIndividual'] == 'individual']


# **Remove unwanted columns**

# In[ ]:


k = k.drop(['isIndividual','col4','col5','col6','col7','col8','col9','col10','col11'], 1)


# **Get the name into the right format - name fragments and last name**

# In[ ]:


#Remove double quotes, commas, incorrect values from Name
k["Name"] = k["Name"].str.replace(',', '')
k["Name"] = k["Name"].str.replace('"', '')
k["Name"] = k["Name"].str.replace('(previously listed as)', '')


# In[ ]:


#create a new dataframe p - copy of k
p = pd.DataFrame(k.SanctionID)
p['Name'] = pd.DataFrame(k.Name)


# In[ ]:


#split the names and get the last word as last name


# In[ ]:


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


u['NameFragment'] =u.Name.str.rsplit(' ',1).str[0]


# In[ ]:


u = u.drop('NameFragment', axis=1).join(u['NameFragment'].str.split(' ', expand=True).stack().reset_index(level=1, drop=True).rename('NameFragment'))


# In[ ]:


u['LastName'] = '0'
#final Namefragment for firstnames and LastName = 0 column


# **Join all name fragments and last name column from above two tables into original df k**

# In[ ]:


s=k


# In[ ]:


s = s.drop(['details'],1)
s = s.join(p.set_index('SanctionID')[['NameFragment']], on='SanctionID')


# In[ ]:


s = s.join(p.set_index('SanctionID')[['LastName']], on='SanctionID')
#change lastname to namefragments later


# In[ ]:


frames  = [s, u]
result = pd.concat(frames)


# In[ ]:


#sort
result = result.sort_index( ascending=[True])


# In[ ]:


result = result.join(k.set_index('SanctionID')[['details']], on='SanctionID')


# In[ ]:


k=result


# **Create a new dataframe with just the details column to extract individual values (DOB and COB) into new columns**

# In[ ]:


d= k[~k.index.duplicated(keep='first')]


# In[ ]:


l = pd.DataFrame(d.details)


# **Get first value by splitting details with ';' (i.e. DOB mostly)**

# In[ ]:


l['FirstValue'] = d['details'].str.rsplit(';').str[0] 


# In[ ]:


#Create a new column which only gets the DOB from firstvalue and gives null for others/missing values


# In[ ]:


l['DOBString'] = np.where(~l['FirstValue'].str.contains("DOB") == True, "null", l['FirstValue'])


# **Remove "DOB " and "circa " from DOB values and just keep the date**

# In[ ]:


l["DOBString"] = l["DOBString"].str.replace("DOB ", "")
l["DOBString"] = l["DOBString"].str.replace("circa ", "")


# In[ ]:


#Create DOB from DOBString
l['DOB']=l['DOBString']


# In[ ]:


#if "to" in dob, give null (to remove the between DOB values from DOB column)
l['DOB'] = np.where(~l['DOB'].str.contains("to") == True, l['DOB'],"None")


# **Add the new DOB column in 'l' into the original dataframe 'k'**

# In[ ]:


k['DOBString']=l.DOBString
k['DOB']=l.DOB


# In[ ]:


#convert DOB to date, change format


# In[ ]:


k['DOB'] = pd.to_datetime(k['DOB'],errors='coerce')
k['DOB']= k["DOB"].dt.strftime('%Y-%m-%d')


# In[ ]:


#Extract MOB, YOBEnd, and YOB Start from DOB


# In[ ]:


k['MOB'] = pd.DatetimeIndex(k['DOB']).month
k['YOBEnd'] = pd.DatetimeIndex(k['DOB']).year
k['YOBStart'] = k['YOBEnd']


# **Fill null/None values of int type with 0**

# In[ ]:


k.DOB.fillna(0,inplace=True)
k.MOB.fillna(0,inplace=True)
k.YOBEnd.fillna(0,inplace=True)
k.YOBStart.fillna(0,inplace=True)


# In[ ]:


#convert from float64 to int64
k['MOB']=k['MOB'].astype(np.int64)
k['YOBStart']=k['YOBStart'].astype(np.int64)
k['YOBEnd']=k['YOBEnd'].astype(np.int64)


# **Extract POB data from details column**

# In[ ]:


#Split the data at the keyword 'POB'
l = l.details.str.split("POB",expand=True)


# In[ ]:


#POB is now in the 2nd column with rest of the values after it
l.columns =['TextbeforePOB', 'POBAndOther', 'col3','col4','col5','col6']


# In[ ]:


#Remove rest of the values from POB (this is after the ';')


# In[ ]:


l = l.POBAndOther.str.split("; ",expand=True)


# In[ ]:


#Rename the first column which now has POB
l = l.rename(columns={0: 'POB'})


# In[ ]:


#Only get the POB Country i.e. get the last value only


# In[ ]:


l['COB'] =  l['POB'].str.split().str[-1]


# **Add the new POB column in 'l' into the original dataframe 'k'**

# In[ ]:


k['COB']=l.COB


# **Remove unwanted columns**

# In[ ]:


k = k.drop(['details'], 1)


# **Bring in the address data from add.csv and join based on ID**

# In[ ]:


colnames=['SanctionID', 'id2', 'Address', 'col4','col5','col6']
df = pd.read_csv('add.csv',sep=',', header=None,  names=colnames)


# In[ ]:


k = k.join(df.set_index('SanctionID')[['Address']], on='SanctionID')


# **Replace '-0-' in Address with null so the empty values are reffered as null for consistenty**

# In[ ]:


k["Address"] = k["Address"].str.replace("-0- ", "null")


# In[ ]:


k = k.sort_index(ascending=[True])


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


k['Country'] = 'US'


# **Swap the columns**

# In[ ]:


k = k[["Country","SanctionID","Name", "NameFragment", "LastName", "DOBString", "DOB", "MOB","YOBStart","YOBEnd", "COB", "Address"]]


# **Final: Export Dataframes into csv and create a log file**

# In[ ]:


#entNames


# In[ ]:


entNames = k[['Country','SanctionID','Name','NameFragment','LastName','COB','Address']]


# In[ ]:


entNames.to_csv('entNames.csv', index=False)


# In[ ]:


#entDOB


# In[ ]:


entDOB = k[['Country','SanctionID','DOBString','DOB','MOB','YOBStart','YOBEnd']]


# In[ ]:


entDOB = entDOB[~entDOB.index.duplicated(keep='first')]


# In[ ]:


entDOB.to_csv('entDOB.csv', index=False)


# In[ ]:


#entSanctioned


# In[ ]:


entSanctioned = k[['Country','SanctionID','Name','DOBString']]


# In[ ]:


entSanctioned = entSanctioned[~entSanctioned.index.duplicated(keep='first')]


# In[ ]:


entSanctioned.to_csv('entSanctioned.csv', index=False)


# In[ ]:


#Logfile


# In[ ]:


#entDOB
entNamesindex = entNames.index
number_of_rowsNames = len(entNamesindex)


# In[ ]:


#entDOB
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

