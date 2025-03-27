import generic_module
import pandas as pd
from datetime import datetime, timedelta

#----------------------------------------------------------------------------
#server_name = generic_module.config.server_name.prod
#database_name = generic_module.config.server_name.database_name
server_name = "uldwh02"
database_name = "db-au-cmdwh"

historical_data_sql="""
select 
       year(QuoteDate) 'Year',month(QuoteDate) 'Month',day(QuoteDate) 'Day',o.GroupName,sum(q.QuoteCount) 'count'
from 
        penQuoteSummary q WITH (NOLOCK)
join 
        penOutlet o WITH (NOLOCK) on q.OutletAlphaKey = o.OutletAlphaKey
where 
        OutletStatus = 'Current' and QuoteDate between dateadd(m,-1,getdate()) and  getdate()-1 
group by 
        year(QuoteDate),month(QuoteDate),day(QuoteDate),o.GroupName
"""

historical_data_df = generic_module.execute_sql_and_return_result(server_name,database_name,historical_data_sql)
historical_data_stats_df = generic_module.calculate_thresholds(historical_data_df,3)
print(historical_data_stats_df.head())

#----------------------------------------------------------------------------
# Get today's date
today_date = datetime.now().date()
# Calculate one day before today's date
one_day_before_date = today_date - timedelta(days=1)

# Define the SQL query with parameter placeholders
# Add with no lock
sql_query = f"""
select 
       year(QuoteDate) 'Year',month(QuoteDate) 'Month',day(QuoteDate) 'Day',o.GroupName,sum(q.QuoteCount) 'count'
from 
        penQuoteSummary q WITH (NOLOCK)
join 
        penOutlet o WITH (NOLOCK) on q.OutletAlphaKey = o.OutletAlphaKey
where 
        OutletStatus = 'Current' and convert(varchar(20),QuoteDate,23) = {"'"+ str(one_day_before_date)+ "'"}
group by 
        year(QuoteDate),month(QuoteDate),day(QuoteDate),o.GroupName
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
#table_name = generic_module.config.quote_table_name
server_name = "uldwh02"
database_name = "db-au-workspace"
table_name = "tbl_recon_analytics_quote"
generic_module.insert_dataframe_to_sqlserver(merged_df, server_name, database_name, table_name)


#----------------------------------------------------------------------------
# Uncomment the below code if you want to save an excel file which has two sheet 1st sheet shows count > upper threshold and 2nd sheet shows count<lower threshold

current_data_date = one_day_before_date
df = merged_df
policy_or_quote = "quote"

#generic_module.write_to_excel(current_data_date, df, policy_or_quote)