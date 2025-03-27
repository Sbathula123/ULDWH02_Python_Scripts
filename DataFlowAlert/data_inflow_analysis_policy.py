"""
Specify the server name, these server names are stored in the yml file

"""
import generic_module
import pandas as pd
from datetime import datetime, timedelta

#----------------------------------------------------------------------------
#server_name = generic_module.config.server_name.prod
#database_name= generic_module.config.server_name.database_name
server_name = "uldwh02"
database_name= "db-au-cmdwh"
# sql="""select year(IssueDate) 'Year',month(IssueDate) 'Month',day(IssueDate) 'Day',\
#     o.GroupName,count(p.PolicyNumber) as count from penPolicy p join penOutlet\
#           o on p.OutletAlphaKey = o.OutletAlphaKey\
#             where OutletStatus = 'Current' and year(IssueDate) = '2023'\
#                 group by year(IssueDate),month(IssueDate),day(IssueDate),o.GroupName"""

# sql="""select year(IssueDate) 'Year',month(IssueDate) 'Month',day(IssueDate) 'Day',
#     o.GroupName,count(p.PolicyNumber) as count from penPolicy p join penOutlet
#           o on p.OutletAlphaKey = o.OutletAlphaKey
#             where OutletStatus = 'Current' and IssueDate >= DATEADD(MONTH, -1, GETDATE())
#                 group by year(IssueDate),month(IssueDate),day(IssueDate),o.GroupName
# """

sql="""
select 
      year(IssueDateUTC) 'Year',month(IssueDateUTC) 'Month',day(IssueDateUTC) 'Day',o.GroupName,count(p.PolicyNumber) as count 
from 
      penPolicy p WITH (NOLOCK)
join 
      penOutlet o WITH (NOLOCK) on p.OutletAlphaKey = o.OutletAlphaKey
where 
      OutletStatus = 'Current' and IssueDateUTC between dateadd(m,-1,getdate()) and  getdate()-1
group by 
      year(IssueDateUTC),month(IssueDateUTC),day(IssueDateUTC),o.GroupName
"""

historical_data_df = generic_module.execute_sql_and_return_result(server_name,database_name,sql)
historical_data_stats_df = generic_module.calculate_thresholds(historical_data_df,3)

#----------------------------------------------------------------------------
# Get today's date
today_date = datetime.now().date()
# Calculate one day before today's date
one_day_before_date = today_date - timedelta(days=1)

# Define the SQL query with parameter placeholders
sql_query = f"""
select 
year(IssueDateUTC) 'Year',month(IssueDateUTC) 'Month',day(IssueDateUTC) 'Day',
    o.GroupName,count(p.PolicyNumber) as count 
from 
      penPolicy p WITH (NOLOCK) 
join 
      penOutlet o WITH (NOLOCK) on p.OutletAlphaKey = o.OutletAlphaKey
where 
      OutletStatus = 'Current' and convert(varchar(20),IssueDateUTC,23) = {"'"+ str(one_day_before_date)+ "'"}
group by 
      year(IssueDateUTC),month(IssueDateUTC),day(IssueDateUTC),o.GroupName
"""
one_day_before_data = generic_module.execute_sql_and_return_result(server_name,database_name,sql_query)

#----------------------------------------------------------------------------
# Left join df1 and df2 on 'Partner' and 'GroupName' columns
merged_df = pd.merge(historical_data_stats_df, one_day_before_data, how='left', left_on=['Partner'], right_on=['GroupName'])

# Fill missing count values with 0
merged_df['count'] = merged_df['count'].fillna(0).astype(int)

# Drop unnecessary columns if needed
merged_df = merged_df.drop(columns=['Year', 'Month', 'Day', 'GroupName'])

# Rename columns as required
merged_df = merged_df.rename(columns={'count': 'IncomingDataCount'})

# Adding the date column to the data frame 
merged_df['Date'] = one_day_before_date

# Reorder the columns
df = merged_df.reindex(columns=['Date', 'Partner', 'UpperThreshold','LowerThreshold','IncomingDataCount'])

# Display the resulting dataframe
print(merged_df.head())

#----------------------------------------------------------------------------

#server_name = generic_module.config.server_name.dev
#database_name = generic_module.config.database_name
#table_name = generic_module.config.policy_table_name
server_name = "uldwh02"
database_name = "db-au-workspace"
table_name = "tbl_recon_analytics2"
generic_module.insert_dataframe_to_sqlserver(merged_df, server_name, database_name, table_name)

#----------------------------------------------------------------------------
# Uncomment the below code if you want to save an excel file which has two sheet 1st sheet shows count > upper threshold and 2nd sheet shows count<loer threshold

current_data_date = one_day_before_date
df = merged_df
policy_or_quote = "policy"

#generic_module.write_to_excel(current_data_date, df, policy_or_quote)
