import json
import requests
import pandas as pd
from requests.exceptions import HTTPError
import os
import datetime
import sys
import traceback

create_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
Log_fp = r"E:\ETL\Data\Oanda_files_test\Log"+create_time+".txt"
if os.path.isfile(Log_fp):
    os.remove(Log_fp)

url_fp = r"E:\ETL\Data\Oanda_files_test\Oanda_1.text"
Output_folder = r"E:\ETL\Data\Oanda_files_test"

# read 200 URLs in a DataFrame
df = pd.read_csv(url_fp, header=None)
df.columns = ["url"]
count = 0
if len(df) != 200:
    print(f"Error: the number of URLS are incorrect.")
    sys.exit(1)
else:
    print(f"**************Start generating json files**************")   
    for i in df.index:
            url = df["url"][i]
            count = count + 1
            response = requests.get(url)
            data = response.content
            #print(data)
            if response.status_code != 200:
                try:
                    response = requests.get(url)
                    # If the response was successful, no Exception will be raised
                    response.raise_for_status()
                except HTTPError as http_err:
                    with open(Log_fp,"a") as g:
                        print(f"[{i}]\tHTTP error occurred: {http_err}\n", file=g)
                       # print(exit)
                        exit()
                except Exception as err:
                    with open(Log_fp,"a") as g:
                        print(f"[{i}]\tOther error occurred: {err}\n", file=g)
                        #print(exit)
                        exit()
            if os.path.exists(Log_fp) and os.path.getsize(Log_fp) !=0: 
                print(f"[{count-1}] Jason files generated successfully but there is an Error in the line [{count}] URL, full process is unsuccessful. Please refer to log for the error: "+ Log_fp)
                sys.exit(1)
            elif response.status_code == 200:
                with open(Output_folder+"\Oanda_test"+str(i)+".json","wb") as f:
                    f.write(data)
print(f"[{count}] Oanda json files generated successfully!") 