# Custom class to connect and interact with the Mongo data base
from datetime import datetime
from MongoDB import MongoDB
import constants as cnt

import json
import pprint
import urllib
import urllib.request


class openDataCollector:

    apiUrl = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?'

    sourceName = 'opendatabcn'

    resourceCollectionBirths = 'births'
    resourceNameBirths = 'naixements_sexe'
    resourceDictBirths = {
        '2007': '1b1bdb74-6078-40e8-a002-fcff87295688',
        '2008': 'f6029e43-e5f9-4623-9c23-bf0aba7dd39b',
        '2009': '6b8606e2-109d-4a1c-9af3-a97de8751a6c',
        '2010': 'ad8dc739-25aa-4720-a971-e27225e26eb8',
        '2011': 'd3901a2b-d686-4267-871f-7059f2c960d1',
        '2012': '48343cec-f34c-4b65-bee7-9696f9220fc9',
        '2013': '15771cd9-e38d-4c93-8e61-fd5bafe0db1e',
        '2014': 'd1880efd-2c68-4dea-9bc4-20b136e0fb62',
        '2015': '3d18a4fc-7d46-4e39-a4c4-7b0e368b11bc',
        '2016': 'a0b95f3f-2537-41cc-b82d-8092785b8b79',
        '2017': 'a405f08b-60a4-4378-867d-f41201dceae9',
        '2018': '091e6a7f-9454-4c31-834b-a27ad9062952',
        '2019': '83a0195f-ce6f-4bcc-9ffb-b863ab5de468',
        '2020': '6f4cbe8a-58b1-492a-aa5d-b8bbaf8a9987'
        
    }

    resourceCollectionAvgRent = 'opendatabcn_rent'
    resourceNameAvgRent = 'lloguer_preu_trim'
    resourceDictAvgRent = {
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

    def getIngestionTimestamp(self):
        
        return datetime.now().strftime(r"%Y-%m-%dT%H.%M.%S.%f")[:-3]

    def openDataAPICollector(self, source='rent'):

        print('Collecting rent data from API...')

        if source == 'rent':
            collectionName = self.resourceCollectionAvgRent
            resourceDict = self.resourceDictAvgRent
            resourceName = self.resourceNameAvgRent

        elif source == 'births':
            collectionName = self.resourceCollectionBirths
            resourceDict = self.resourceDictBirths
            resourceName = self.resourceNameBirths

        if not self.db.collection_exists(collectionName):
            self.db.create_collection(collectionName)

        jsonList = []
        for resource_year, resource_url in resourceDict.items():
            
            params = {
                'resource_id': resource_url,
                'limit': 32000 # upper limit by default in CKAN Data API 
            }

            url = self.apiUrl + urllib.parse.urlencode(params)

            response = urllib.request.urlopen(url)
            data_json = json.loads(response.read())
            
            if data_json.get('success') == True:

                jsonFile = {
                    '_id': self.sourceName + '$'+ resourceName +'$' + self.getIngestionTimestamp(),
                    'value': {
                        'data': data_json.get('result'),
                        'metadata': {
                            'year': resource_year
                        }
                    }
                }
                #pprint.pprint(data_json)

                jsonList.append(jsonFile)

        self.db.insert_many(collectionName, jsonList)

        print('Data stored succesfully.')

        return True

if __name__ == "__main__":

    dataCollector = openDataCollector()

    dataCollector.openDataAPICollector()