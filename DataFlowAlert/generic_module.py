#import yaml
import math
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine

#----------------------------------------------------------------------------
class Config:
    def __init__(self, dictionary):
        for key, value in dictionary.items():
            if isinstance(value, dict):
                setattr(self, key, Config(value))
            else:
                setattr(self, key, value)

#----------------------------------------------------------------------------
#def load_yaml_file(file_path):
#    with open(file_path, 'r') as file:
#        data = yaml.safe_load(file)
#    return data

#----------------------------------------------------------------------------
# Load YAML data from file
#yaml_file_path = 'config.yml'
#yaml_data = load_yaml_file(yaml_file_path)

# Create a Config instance with the loaded YAML data
#config = Config(yaml_data)

#----------------------------------------------------------------------------
#this function returns the result of query exeuction as a dataframe.

def execute_sql_and_return_result(server_name,database_name,sql):
    try:
        engine = create_engine('mssql+pymssql://@'+server_name+':1433/'+database_name, pool_recycle=3600, pool_size=5)
        if "(NOLOCK)" not in sql.upper():
            print("WARNING: USE 'WITH (NOLOCK)' HINT IN SQL TO AVOID LOCKING ISSUES.")
        df_result=pd.read_sql(sql,engine)
        print(df_result.head(5))
        engine.dispose()
        print('SQL is executed. Returning result.')
        return(df_result)
    except Exception as excp:
        raise Exception("Error while executing execute_sql_and_return_result(). Full error description is:"+str(excp))
    
#----------------------------------------------------------------------------
def calculate_thresholds(df, std_dev_multiplier=1):
    # List to store threshold data
    threshold_data = []

    # Iterate over unique GroupName values
    for group_name in df['GroupName'].unique():
        # Filter the DataFrame for the current group name
        filtered_df = df[df['GroupName'] == group_name]

        # Extract data for the current group
        group_data = filtered_df['count'].values

        # Calculate the first and third quartiles
        q1 = np.percentile(group_data, 25)
        q3 = np.percentile(group_data, 75)

        # Calculate the interquartile range (IQR)
        iqr = q3 - q1

        # Define the lower and upper bounds for outliers using the IQR method
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Filter out outliers
        filtered_data = group_data[(group_data >= lower_bound) & (group_data <= upper_bound)]

        # Calculate the mean and standard deviation for the filtered data
        outlier_mean_value = np.mean(filtered_data)
        outlier_std_deviation = np.std(filtered_data)

        # Calculate upper and lower thresholds based on mean and standard deviation of filtered data
        outlier_threshold_upper = outlier_mean_value + std_dev_multiplier * outlier_std_deviation
        outlier_threshold_upper = math.floor(outlier_threshold_upper)
        outlier_threshold_lower = outlier_mean_value -  outlier_std_deviation
        outlier_threshold_lower = math.floor(outlier_threshold_lower)

        # Append threshold data to the list
        threshold_data.append({
            'Partner': group_name,
            'UpperThreshold': outlier_threshold_upper,
            'LowerThreshold': outlier_threshold_lower
        })

    # Create DataFrame from the threshold data
    threshold_df = pd.DataFrame(threshold_data)
    
    return threshold_df

#----------------------------------------------------------------------------

def insert_dataframe_to_sqlserver(dataframe, server_name, database_name, table_name):
    """
    Insert a DataFrame into a SQL Server database table.

    Args:
    - dataframe: The DataFrame to insert.
    - server_name: The name of the SQL Server instance.
    - database_name: The name of the database in the SQL Server instance.
    - table_name: The name of the table to insert the data into.
    """

    
    # Create the SQLAlchemy engine
    engine = create_engine('mssql+pymssql://@'+server_name+':1433/'+database_name, pool_recycle=3600, pool_size=5)
    
    try:
        # Insert DataFrame into SQL Server database table
        dataframe.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Data has been sent to the database")
    except Exception as e:
        print("Error occurred while inserting data:", e)
    finally:
        # Close the database connection
        engine.dispose()

#----------------------------------------------------------------------------
def write_to_excel(current_data_date, df, policy_or_quote):
   
    # Create a new Excel writer object
    file_name = f"{current_data_date}_{policy_or_quote}.xlsx"
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    
    # Filter rows where IncomingDataCount is greater than UpperThreshold
    filtered_df_upper = df[df['IncomingDataCount'] > df['UpperThreshold']].copy()
    # Drop the LowerThreshold column
    filtered_df_upper.drop(columns=['LowerThreshold'], inplace=True)
    df.loc[filtered_df_upper.index, 'LowerThreshold'] = None
    
    # Write data and DataFrame to Excel
    filtered_df_upper.to_excel(writer, sheet_name='DataCount_GT_Upper', index=False)

    # Add header and footer
    worksheet_upper = writer.sheets['DataCount_GT_Upper']
    worksheet_upper.set_column('A:B', 20)


    # Filter rows where IncomingDataCount is less than LowerThreshold
    filtered_df_lower = df[df['IncomingDataCount'] < df['LowerThreshold']].copy()
    # Drop the UpperThreshold column
    filtered_df_lower.drop(columns=['UpperThreshold'], inplace=True)
    df.loc[filtered_df_lower.index, 'UpperThreshold'] = None
    
    # Write data and DataFrame to Excel
    filtered_df_lower.to_excel(writer, sheet_name='DataCount_LT_Lower', index=False)

    # Add header and footer
    worksheet_lower = writer.sheets['DataCount_LT_Lower']
    worksheet_lower.set_column('A:B', 20)

    
    # Save the Excel file
    writer.save()