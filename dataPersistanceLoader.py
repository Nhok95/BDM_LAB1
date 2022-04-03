# Custom class to connect and interact with the Mongo data base
from datetime import datetime
from MongoDB import MongoDB
import constants as cnt

from pprint import pprint


class dataPersistanceLoader:

    collectionName = cnt.RENT_COLLECTION_NAME

    def __init__(self):
        self.db_temporal = MongoDB(cnt.HOST, cnt.PORT, db_name = cnt.TEMPORAL_DB)
        self.db = MongoDB(cnt.HOST, cnt.PORT, db_name = cnt.PERSISTENT_DB)

    def getIngestionTimestamp(self):
        
        return datetime.now().strftime(r"%Y-%m-%dT%H.%M.%S.%f")[:-3]

    def dataAPIPersistanceLoader(self):

        print('Loading data from the temporal landing...')

        temporalFiles = self.db_temporal.find(self.collectionName)
        temporalLength = self.db_temporal.count_documents(self.collectionName)

        #print(type(temporalFiles[0]))
        #pprint(temporalFiles[0])

        jsonList = []
        for doc in temporalFiles:

            #key = doc.get('_id').split('$')
            data = doc.get('value').get('data')
            metadata = doc.get('value').get('metadata')

            key_id = metadata.get('sourceName') + '$' + \
                     data.get('resource_id') + '$' + \
                     self.getIngestionTimestamp()

            jsonFile = {
                '_id': key_id,
                'value' : {
                    'data': data.get('records'),
                    'metadata': {
                        'schema': data.get('fields'),
                        'year': metadata.get('year'),
                        'resource_id': data.get('resource_id'),
                        'totalRecords': data.get('total')     
                    }
                }
            }

            jsonList.append(jsonFile)
        

        #pprint(jsonList[0])
        # Same number of documents 
        if temporalLength == len(jsonList):
            self.db.insert_many(self.collectionName, jsonList)

            self.db_temporal.delete_collection(self.collectionName)

            print( 'Data stored succesfully. {} documents inserted.'.format(len(jsonList)) )
        
        else:
            print("Error inserting into the persistent zone")

        #print(temporalLength)

        # We clean the collection once we procees them 
        # We check if we have the same number of documents
        #if len(temporalFiles) == len(jsonList):
            #self.db_temporal.
            

        return True

if __name__ == "__main__":

    dataLoader = dataPersistanceLoader()

    dataLoader.dataAPIPersistanceLoader()