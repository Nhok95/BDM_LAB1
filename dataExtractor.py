import os
from os.path import join, isfile
import json
import re
import pandas as pd

class dataExtractor:

    sourcePath = join(os.getcwd(),'Sources')

    openDataIncomePath = join(sourcePath,'opendatabcn-income')

    def __init__(self):
        pass

    def idealistaDataExtractor(self):

        idealistaList = []
        idealistaPath = join(self.sourcePath,'idealista')
        
        jsonFiles = [f for f in os.listdir(idealistaPath) if isfile(join(idealistaPath, f))]

        for jsonFile in jsonFiles:

            if jsonFile == '2020_01_02_idealista.json':

                #YYYY_MM_DD_idealista.json -> YYYYMMDD
                date = re.search(r'\d{4}_(0[1-9]|1[0-2])_(0[1-9]|[12][0-9]|3[01])', jsonFile).group(0)    
                date = re.sub(r'_', r'', date)

                # Opening JSON file
                print("### {} ###".format(date))
                
                f = open(join(self.idealistaPath, jsonFile))
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

                        idealistaList.append(jsonEntity)

                        print(propertyJSON)
                        '''
                        print("propertyCode: {}".format(propertyJSON['propertyCode']))
                        print("district: {}".format(propertyJSON['district']))
                        print("neighborhood: {}".format(propertyJSON['neighborhood']))
                        print("latitude: {}".format(propertyJSON['latitude']))
                        print("longitude: {}".format(propertyJSON['longitude']))
                        '''
                        print()
                
                # Closing file
            f.close()

        return idealistaList

    def lookUpExtractor(self):

        lookUpList = []
        IdealistaLookUpPath = join(self.sourcePath,'lookup_tables')

        idealista_csv = pd.read_csv(open(join(IdealistaLookUpPath, 'idalista_extended.csv')), delimiter=',')

        


        income_csv = pd.read_csv(open(join(IdealistaLookUpPath, 'income_opendatabcn_extended.csv')), delimiter=',')
        


        return lookUpList




if __name__ == "__main__":

    dataExtractor = dataExtractor()

    idealista = dataExtractor.idealistaDataExtractor()
    lookUp = dataExtractor.lookUpExtractor()

    print(len(idealista))
    print(len(lookUp))