import pandas as pd

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


# Assuming 'output.csv' contains the DataFrame
df = pd.read_csv('output.csv')

policy_or_quote ='Policy'
current_data_date ='2024-04-19'
write_to_excel(current_data_date, df, policy_or_quote)
