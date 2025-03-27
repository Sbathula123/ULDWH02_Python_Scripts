#!/usr/bin/env python
# coding: utf-8

"""
    **Details:** 
        Code to extract the sanction list (individuals) from "https://www.gov.uk/government/publications/the-uk-sanctions-list; (link 2 UK)" into the appropriate format required by the existing BI tables entSanctionedNames and entSanctionedDOB

    **Author:** 
        Thahaseena KP (Capgemini)

    **Last code updated date**: 
        03/12/2021 Thahaseena KP (Capgemini):
            - initial code
        03/03/2022 Oscar Gardner (Capgemini): 
            - removed jupyter notebook comment artifacts, renamed 3 columns and recreated 'Primary Name' 
            column due to a change in the source data format as of 28 Feb 2022
            - rewrote docstring
        02/05/2022 Oscar Gardner (Capgemini):
            - refactored date parsing logic to be more robust and to fix a bug causing errors downstream
            - refactored file input code to use the .ods file directly. The script becomes slower by about 4 minutes, but since it is now fully automated the speed decrease doesn't matter.

    **Required files in the same folder:** 
        uk-sanctions-list.xlsx 
    
    **Output files:** 
        entNames.csv, entDOB.csv, entSanctioned.csv, EntTablesNoOfRows.txt
"""
# **Load the sanction list/data**
import pandas as pd
import numpy as np
from datetime import datetime
import re
from pathlib import Path

input_path = Path(r"E:\ETL\Sanctioned\Source Files\uk_sanctions_list.ods")
csv_output_dir = Path(r"E:\ETL\Sanctioned\Converted Files")

# full csv output paths
ent_names_path = csv_output_dir / Path("entNames_Link2.csv")
ent_dob_path = csv_output_dir / Path("entDOB_Link2.csv")
ent_sanctioned_path = csv_output_dir / Path("entSanctioned_Link2.csv")

logfile_path = Path(r"E:\ETL\Sanctioned\Python Scripts\EntTablesNoOfRows.txt")

df = pd.read_excel(input_path,'Individual', skiprows=2)

#Remove duplicate records
df = df[~df['OFSI Group ID'].duplicated(keep='first')]


def create_primary_name(row):
    """ Primary Name column was removed, so this code uses the Name columns (Name 1 ... Name 6) 
        to recreate it. (which as of 28 Feb 2022 now contain correct data)
    
    This solution minimises changes to the later code.
    """
    names = [row["Name 1"], row["Name 2"], row["Name 3"], row["Name 4"], row["Name 5"], row["Name 6"]]
    return " ".join(name for name in names if isinstance(name, str))

df["Primary Name"] = df.apply(create_primary_name, axis=1)

# 28 Feb 2022 data source renamed a few columns, this code renames it back for output file consistency
df.rename(columns={
    "Address Postal Code": "Postcode",
    "OFSI Group ID": "OFSI ID",
    "Address Country":"Primary Address Country"
    }, inplace=True
)

k = pd.DataFrame(df['OFSI ID'])
k[['Name','DOBString','COB']] = df[['Primary Name','D.O.B','Country of birth']]
l = pd.DataFrame(df['OFSI ID'])
l[['Address Line 1','Address Line 2','Address Line 3','Address Line 4', 'Postcode', 'Primary Address Country']] = df[['Address Line 1','Address Line 2','Address Line 3','Address Line 4', 'Postcode', 'Primary Address Country']]

#Combine Address
k['Address'] = l[['Address Line 1','Address Line 2','Address Line 3','Address Line 4', 'Postcode', 'Primary Address Country']]\
    .apply(lambda x: '|'.join([str(ele).replace("|","").strip(", ") for ele in x.dropna()]), axis=1)
k["Address"] = k["Address"].str.replace('Unknown', '')
k["Address"] = k["Address"].str.replace('unknown', '')
k["Address"] = k["Address"].str.replace('|', '')
k = k.replace(r'^\s*$', np.nan, regex=True)

#Replace unknown values as null
k["COB"] = k["COB"].str.title().replace('Unknown', 'null')
k["DOBString"] = k["DOBString"].str.title().replace('Unknown', 'null')
k.rename(columns = {'OFSI ID':'SanctionID'}, inplace = True)

# **Get the columns into a new df, so that it can be added after name fragment split**

#Save Address, DOBString & COB to a new df columns
i = pd.DataFrame(k.SanctionID)
i['Address']=k['Address']
i['DOBString']=k['DOBString']
i['COB']=k['COB']

#Remove Address, DOBString & COB from k
k = k.drop(['Address'],1)
k = k.drop(['DOBString'],1)
k = k.drop(['COB'],1)

# **Split the name into fragments and get last name**

#Remove double quotes, commas, and incorrect values from Name
k["Name"] = k["Name"].str.replace(',', '')
k["Name"] = k["Name"].str.replace('"', '')
k["Name"] = k["Name"].str.replace('(previously listed as)', '')
p = pd.DataFrame(k.SanctionID)
p['Name'] = pd.DataFrame(k.Name)

#Remove unwanted characters
p['Name'] = p["Name"].str.replace('-', '')
p['Name'] = p["Name"].str.replace("'", "")

#split the names and get the last word as last name
p['NameFragment'] = p['Name'].str.rsplit(' ').str[-1] 
p['LastName'] = '1'

#final Namefragment for lastnames and LastName = 1 column

#create a copy to extract firstname name fragments
u=p

#Remove Lastname fragments and lastName column so this can be used for Firstname fragments and lastname=0
u = u.drop(['NameFragment','LastName'],1)

#Remove single word names as they are considered as lastnames
u = u[u.Name.str.count(' ') > 0]

#Split the names (without last word)
u['NameFragment'] =u.Name.str.rsplit(' ',1).str[0]

#Get Firstname fragments
u = u.drop('NameFragment', axis=1).join(u['NameFragment'].str.split(' ', expand=True).stack().reset_index(level=1, drop=True).rename('NameFragment'))
u['LastName'] = '0'

#final Namefragment for firstnames and LastName = 0 column

#Remove name column from u and copy it again from k (k has "'" and "-")
u = u.drop(['Name'],1)
u['Name']=k['Name']

# **Join all name fragments and last name column from above two tables into original df k**
s=k
s = s.join(p.set_index('SanctionID')[['NameFragment']], on='SanctionID')
s = s.join(p.set_index('SanctionID')[['LastName']], on='SanctionID')
frames  = [s, u]
result = pd.concat(frames, sort=True)

#sort
result = result.sort_index( ascending=[True])
k=result

#Add in the removed columns back to k, fills them on each namefragment rows
k['Address'] = i['Address']
k['COB'] = i['COB']
k['DOBString'] = i['DOBString']

# **DOB**


# todo completely rewrite date parsing as it's completely wrong. the current logic misses 90% of values
def convert_to_datetime(dt_str):
    # currently need a way to ensure that MOB isn't filled with a fake value?
    if isinstance(dt_str, str):
        try:
            return datetime.strptime("%d/%m/%Y", dt_str)
            return datetime.strptime("%Y/%m/%d", dt_str)
        except ValueError:
            # none of the above two were matches, continue
            pass
    
        matches = re.findall(r"dd/(\d{1,2})/(\d{2,4})", dt_str, flags=re.IGNORECASE)
        if matches:
            return datetime(year=int(matches[0][1]), month=int(matches[0][0]), day=1)
        
        matches = re.findall(r"dd/mm/(\d{2,4})", dt_str, flags=re.IGNORECASE)
        if matches:

            return datetime(year=int(matches[0]), month=1, day=1)
            
        matches = re.findall(r"(\d{4})", dt_str, flags=re.IGNORECASE)
        if matches:
            
            return datetime(year=int(matches[0]), month=1, day=1)
        
    # no matches found, or wasn't a string at all (e.g. a None or np.NaN type)
    return False   
    

  
#Create DOB from DOBString
#k['DOB']=k['DOBString']
k["DOB"] = k["DOBString"].apply(convert_to_datetime)
#k['DOB'] = pd.to_datetime(k['DOB'],errors='coerce')

#Extract MOB, YOBEnd, and YOB Start from DOB
#k['MOB'] = pd.DatetimeIndex(k['DOB']).month
k['MOB']= k["DOB"].apply(lambda dt: dt.month if dt else None)
k['YOBEnd']= k["DOB"].apply(lambda dt: dt.year if dt else None)

k['YOBStart'] = k['YOBEnd']

k['DOB']= k["DOB"].apply(lambda dt: datetime.strftime(dt, '%Y-%m-%d') if dt else None)

# ULDWH02 stored procedure etlsp_cmdwh_entSanctionedDOB_New expects null values to be denoted with the string "NaT"
k.DOB.fillna("NaT",inplace=True)

#Fill null/None values with 0
# k.DOB.fillna(0,inplace=True)

k.MOB.fillna(0,inplace=True)
k.YOBEnd.fillna(0,inplace=True)
k.YOBStart.fillna(0,inplace=True)

#convert from float64 to int64
k['MOB']=k['MOB'].astype(np.int64)
k['YOBStart']=k['YOBStart'].astype(np.int64)
k['YOBEnd']=k['YOBEnd'].astype(np.int64)

# **Fill null/None values of string type with null**
k.DOBString.fillna('null',inplace=True)
k.COB.fillna('null',inplace=True)
k.Address.fillna('null',inplace=True)

# **Remove double quotes, comma, and line breaks**
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
k['COB'] = k['COB'].str[-30:]

# **Add Country column**
k['Country'] = 'UK'

# **Swap the columns**
k = k[["Country","SanctionID","Name", "NameFragment", "LastName", "DOBString", "DOB", "MOB","YOBStart","YOBEnd", "COB","Address"]]

# **Final: Export Dataframes into csv and create a log file**

#entNames
entNames = k[['Country','SanctionID','Name','NameFragment','LastName','COB','Address']]
entNames.to_csv(ent_names_path, index=False)

#entDOB
entDOB = k[['Country','SanctionID','DOBString','DOB','MOB','YOBStart','YOBEnd']]
entDOB = entDOB[~entDOB.index.duplicated(keep='first')]
entDOB.to_csv(ent_dob_path, index=False)

#entSanctioned
entSanctioned = k[['Country','SanctionID','Name','DOBString']]
entSanctioned = entSanctioned[~entSanctioned.index.duplicated(keep='first')]
entSanctioned.to_csv(ent_sanctioned_path, index=False)

#Logfile

#entNames
entNamesindex = entNames.index
number_of_rowsNames = len(entNamesindex)

#entNames
entDOBindex = entDOB.index
number_of_rowsDOB = len(entDOBindex)

#entDOB
entDOBindex = entDOB.index
number_of_rowsSanctioned = len(entDOBindex)

now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
c="Date and time: " + dt_string + " Total No Of Rows:- entNames: " + str(number_of_rowsNames) + " entDOB: " + str(number_of_rowsDOB), "entSanctioned: " + str(number_of_rowsSanctioned)
c =str(c)

#Open a file with access mode 'a'
with open(logfile_path, "a",newline='\n') as file_object:
    file_object.write(c+"\n")