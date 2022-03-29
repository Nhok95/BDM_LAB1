import os
from os.path import join, isfile
import json
import re

class dataExtractor:

    sourcePath = join(os.getcwd(),'Sources')

    idealistaPath = join(sourcePath,'idealista')
    openDataIncomePath = join(sourcePath,'opendatabcn-income')
    lookUpPath = join(sourcePath,'lookup_tables')

    def __init__(self):
        pass

    def idealistaDataExtractor(self):

        idealistaList = []
        jsonFiles = [f for f in os.listdir(self.idealistaPath) if isfile(join(self.idealistaPath, f))]

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

        print(len(idealistaList))


if __name__ == "__main__":

    dataExtractor = dataExtractor()

    idealista = dataExtractor.idealistaDataExtractor()