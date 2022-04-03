# Custom class to connect and interact with the Mongo data base
from MongoDB import MongoDB
import constants as cnt

import json
import pprint
import urllib
import urllib.request


class openDataCollector:

    apiUrl = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?'

    collectionName = cnt.RENT_COLLECTION_NAME
    
    sourceName = 'opendatabcn'
    resourceName = 'lloguer_preu_trim'
    resourceDict = {
        '2014': '5855ba6c-f554-4a99-837a-04ea69bc71f4',
        '2015': 'fcdbfa43-d97a-4da3-b78b-6f255dbcf4cc',
        '2016': 'b45e8b56-1988-4474-bf61-0a76f8ab28c2',
        '2017': '0a71a12d-55fa-4a76-b816-4ee55f84d327',
        '2018': '3dc45b16-42a9-4f57-9863-e6d1a4f5869f',
        '2019': '004c76b1-6269-4136-89b2-89fd47046930',
        '2020': '47c9d64d-317a-45d0-8c45-45488df8601c',
        '2021': 'cfc45f2b-62eb-4621-8486-1b90e36b4bfe'
        
    }

    def __init__(self):
        self.db = MongoDB(cnt.HOST, cnt.PORT, db_name= cnt.TEMPORAL_DB)
        

    def openDataAPICollector(self):

        print('Collecting rent data from API...')

        if not self.db.collection_exists(self.collectionName):
            self.db.create_collection(self.collectionName)

        jsonList = []
        for resource_year, resource_url in self.resourceDict.items():
            
            params = {
                'resource_id': resource_url,
                'limit': 32000 # upper limit by default in CKAN Data API 
            }

            url = self.apiUrl + urllib.parse.urlencode(params)

            response = urllib.request.urlopen(url)
            data_json = json.loads(response.read())
            
            if data_json.get('success') == True:

                jsonFile = {
                    'value': {
                        'data': data_json.get('result'),
                        'metadata': {
                            'year': resource_year,
                            'sourceName': self.sourceName,
                            'resourceName': self.resourceName
                        }
                    }
                }
                #pprint.pprint(data_json)

                jsonList.append(jsonFile)

        self.db.insert_many(self.collectionName, jsonList)

        print( 'Data stored succesfully. {} documents inserted.'.format(len(jsonList)) )

        return True

if __name__ == "__main__":

    dataCollector = openDataCollector()

    dataCollector.openDataAPICollector()