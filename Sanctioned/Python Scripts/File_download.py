import requests
import bs4
import pandas as pd
import xml.etree.ElementTree as Xet

#import urlib2
#import BeautifulSoup
#pd.show_versions(as_json=False)

###Link 1####
#Data download for https://www.treasury.gov/ofac/downloads/sdn.csv#
SDN_url = r'https://www.treasury.gov/ofac/downloads/sdn.csv'

req = requests.get(SDN_url)
url_content = req.content
csv_file = open(r'E:\ETL\Sanctioned\Source Files\SDN.csv','wb')

csv_file.write(url_content)
csv_file.close()

#Data download for https://www.treasury.gov/ofac/downloads/add.csv#
ADD_url = r'https://www.treasury.gov/ofac/downloads/add.csv'

req = requests.get(ADD_url)
url_content = req.content
csv_file = open(r'E:\ETL\Sanctioned\Source Files\ADD.csv','wb')

csv_file.write(url_content)
csv_file.close()

###Link 2####
#Data download for https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1028450/uk-sanctions-list.ods#
uk_sanctions_list_url = r'https://www.gov.uk/government/publications/the-uk-sanctions-list'
req = requests.get(uk_sanctions_list_url)
soup = bs4.BeautifulSoup(req.content, 'html.parser')

for a in soup.find_all('a', href = True):
    if a['href'].lower().find("uk_sanctions_list.ods") != -1:
        file_url = a['href']
        
        req = requests.get(file_url)
        #print(req.status_code, file_url)
        
        # avoid redundant duplicate matches and exit the loop now
        break
        
with open(r'E:\ETL\Sanctioned\Source Files\uk_sanctions_list.ods','wb') as outfile:
    outfile.write(req.content)

###Link 3####
#Download data for https://www.dfat.gov.au/sites/default/files/regulation8_consolidated.xls#
regulation_consolidated_url = r'https://www.dfat.gov.au/sites/default/files/regulation8_consolidated.xlsx'

req = requests.get(regulation_consolidated_url)
url_content = req.content
csv_file = open(r'E:\ETL\Sanctioned\Source Files\regulation8_consolidated.csv','wb')
csv_file.write(url_content)
csv_file.close()

#read_file = pd.read_excel(r'E:\ETL\Sanctioned\Source Files\regulation8_consolidated.xls')
#read_file.to_csv(r'E:\ETL\Sanctioned\Source Files\regulation8_consolidated.csv', index = None, header = True)

###Link 5 ####
#Download data for https://www.police.govt.nz/sites/default/files/publications/designated-entities-7-september-2021.xlsx#
designated_entities_url = r'https://www.police.govt.nz/advice/personal-community/counterterrorism/designated-entities/lists-associated-with-resolutions-1267-1989-2253-1988'
req = requests.get(designated_entities_url)

soup = bs4.BeautifulSoup(req.content, 'html.parser')

main_url = 'https://www.police.govt.nz'
for a in soup.find_all('a', href = True):
#    print(a['href'])
    if a['href'].find("xlsx") != -1:
        file_loc = a['href']
        
file_url =  main_url+file_loc
req = requests.get(file_url)
url_content = req.content
csv_file = open(r'E:\ETL\Sanctioned\Source Files\designated-entities.xlsx','wb')

csv_file.write(url_content)
csv_file.close()



#Data download for ttps://scsanctions.un.org/resources/xml/en/consolidated.xml#
#xml_url = r'https://scsanctions.un.org/resources/xml/en/consolidated.xml'

#req = requests.get(xml_url)
#url_content = req.content
#csv_file = open('consolidated.xml','wb')

#csv_file.write(url_content)
#csv_file.close()
