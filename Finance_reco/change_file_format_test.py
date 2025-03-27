import openpyxl

file_path = r'E:\ETL\Siddhesh\Finance_Recon_test.csv'

with open(file_path, 'r', encoding='utf-16') as file:
    content = file.read()


# Split the content into lines
lines = content.split('\n')

workbook = openpyxl.Workbook()
sheet = workbook.active


# Write each line to a separate cell in the same row
row_idx = 1
for line in lines:
    columns = line.split('\t')  # Split columns by tab (adjust as needed)
    col_idx = 1
    for column in columns:
        sheet.cell(row=row_idx, column=col_idx, value=column)
        col_idx += 1
    row_idx += 1

output_path = 'E:\ETL\Siddhesh\Finance_Recon_test.xlsx'
workbook.save(output_path)