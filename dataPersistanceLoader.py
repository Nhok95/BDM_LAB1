# Custom class to connect and interact with the Mongo data base
from MongoDB import MongoDB
import constants as cnt

from pprint import pprint


class dataPersistanceLoader:

    resourceCollectionAvgRent = 'opendatabcn_rent'

    def __init__(self):
        self.db_temporal = MongoDB(cnt.HOST, cnt.PORT, db_name = cnt.TEMPORAL_DB)
        self.db = MongoDB(cnt.HOST, cnt.PORT, db_name = cnt.PERSISTENT_DB)

    def dataAPIPersistanceLoader(self):

        collectionName = self.resourceCollectionAvgRent
        
        temporalFiles = self.db_temporal.find(collectionName)

        temporalLength = self.db_temporal.count_documents(collectionName)

        print(type(temporalFiles[0]))
        pprint(temporalFiles[0])

        jsonList = []
        #for doc in temporalFiles:
        #    print(type(doc))
        #    print()

        '''
        'resource_id': data.get('resource_id'),
        'resource_name': resource_year + resourceName,
        'schema': data.get('fields'),
        'records': data.get('records'),
        'totalRecords': data.get('total')
        '''

        #self.db.insert_many(collectionName, jsonList)

        print(temporalLength)

        # We clean the collection once we procees them 
        # We check if we have the same number of documents
        #if len(temporalFiles) == len(jsonList):
            #self.db_temporal.



            

        return True

if __name__ == "__main__":

    dataLoader = dataPersistanceLoader()

    dataLoader.dataAPIPersistanceLoader()