#!/usr/bin/env python
# coding: utf-8

# **Details:** Code to extract the sanction list (individuals) from "https://www.dfat.gov.au/international-relations/security/sanctions/consolidated-list (link 3 AU)" into the appropriate format required by the existing BI tables entSanctionedNames and entSanctionedDOB
# **Author:** Thahaseena KP (Capgemini)
# **Last code updated date**: 03/12/2021
# **Required files in the same folder:** regulation8_consolidated.xls **Output files:** entNames.csv, entDOB.csv, entSanctioned.csv, EntTablesNoOfRows.txt

# **Load the sanction list/data and give column names**

# In[ ]:


import pandas as pd
import numpy as np
#colnames=['SanctionID', 'Name', 'Type', 'NameType','DOBString','COB','Citizenship','Address','Add info','Listing info','Committees','Control Date']

#df = pd.read_excel(r'E:\ETL\Sanctioned\Source Files\regulation8_consolidated.csv',header=None, names=['SanctionID', 'Name', 'Type', 'NameType','DOBString','COB','Citizenship','Address','Add info','Listing info','Committees','Control Date'])


from spire.xls import *
from spire.xls.common import *

# Initialize an instance of the Workbook class
workbook = Workbook()
# Load an Excel file
workbook.LoadFromFile("E:\ETL\Sanctioned\Source Files\regulation8_consolidated.csv")

# Get the first sheet by its index
sheet = workbook.Worksheets[0]

# Get the number of used columns in the sheet
columnCount = sheet.Columns.Length
print(columnCount)

workbook.Dispose()
print(df)
