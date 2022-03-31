import os
from os.path import join, isfile
import json
import re
import pandas as pd

import pprint

class dataCollector:

    sourcePath = join(os.getcwd(),'Sources')

    openDataIncomePath = join(sourcePath,'opendatabcn-income')

    def __init__(self):
        pass

    def idealistaDataExtractor(self):

        print("Starting data from idealista json files...")

        idealistaList = []
        idealistaPath = join(self.sourcePath,'idealista')
        
        jsonFiles = [f for f in os.listdir(idealistaPath) if isfile(join(idealistaPath, f))]

        for jsonFile in jsonFiles:

            idealistaJsonFile = []

            #YYYY_MM_DD_idealista.json -> YYYYMMDD
            date = re.search(r'\d{4}_(0[1-9]|1[0-2])_(0[1-9]|[12][0-9]|3[01])', jsonFile).group(0)    
            date = re.sub(r'_', r'', date)

            # Opening JSON file
            print("### {} ###".format(date))
            
            f = open(join(idealistaPath, jsonFile))
            file = json.load(f)
            
            # Iterating through the json
            # list
            for propertyJSON in file:

                if propertyJSON.get('neighborhood') != None:
                    
                    jsonEntity = {
                        "propertyCode": propertyJSON.get('propertyCode'),
                        "province": propertyJSON.get('province'),
                        "district": propertyJSON.get('district'),
                        "neighborhood": propertyJSON.get('neighborhood'),
                        "latitude": propertyJSON.get('latitude'),
                        "longitude": propertyJSON.get('longitude'),
                        "date": date
                    }

                    idealistaJsonFile.append(jsonEntity)

                    #print(propertyJSON)
                    '''
                    print("propertyCode: {}".format(propertyJSON['propertyCode']))
                    print("district: {}".format(propertyJSON['district']))
                    print("neighborhood: {}".format(propertyJSON['neighborhood']))
                    print("latitude: {}".format(propertyJSON['latitude']))
                    print("longitude: {}".format(propertyJSON['longitude']))
                    '''
                
            # Closing file
            f.close()

            idealistaList.append(idealistaJsonFile)

        print("Data succesfully extracted.")

        return idealistaList

    def lookUpExtractor(self):

        LookUpPath = join(self.sourcePath,'lookup_tables')

        ##### IDEALISTA LOOK UP TABLE #####
        idealistaDF = pd.read_csv(open(join(LookUpPath, 'idealista_extended.csv')), delimiter=',', encoding='utf8')

        idealistaList = []
        districts = list(set(idealistaDF['district']))
        for district in districts:
            df_by_district = idealistaDF[idealistaDF['district'] == district]
            df_by_district.reset_index(drop=True, inplace=True)

            neighborhoodList = []
            for i in df_by_district.index:
                
                jsonNeighborhood = {
                    "id": df_by_district['neighborhood_id'][i],
                    "name": df_by_district['neighborhood'][i],
                    "normalized_name": df_by_district['neighborhood_n'][i]
                }
                neighborhoodList.append(jsonNeighborhood)

            jsonEntity = {
                "district_id": df_by_district['district_id'][0],
                "name": df_by_district['district'][0],
                "normalized_name": df_by_district['district_n'][0],
                "neighborhood" : neighborhoodList

            }

            # Debug purpouses
            if district == "Nou Barris":
                pprint.pprint(jsonEntity)

        ##### INCOME LOOK UP TABLE #####
        income_csv = pd.read_csv(open(join(LookUpPath, 'income_opendatabcn_extended.csv')), delimiter=',', encoding='utf8')
        


        return idealistaList

    def openDataAPICollector(self):
        return True




if __name__ == "__main__":

    dataCollector = dataCollector()

    #idealista = dataExtractor.idealistaDataExtractor()
    lookUp = dataCollector.lookUpExtractor()

    #print(len(idealista))
    #print(len(lookUp))