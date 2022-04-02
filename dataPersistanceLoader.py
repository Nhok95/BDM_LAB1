# Custom class to connect and interact with the Mongo data base
from MongoDB import MongoDB
import constants as cnt


class dataPersistanceLoader:

    resourceCollectionAvgRent = 'opendatabcn_rent'

    def __init__(self):
        self.db_temporal = MongoDB(cnt.HOST, cnt.PORT, db_name= cnt.TEMPORAL_DB)
        self.db = MongoDB(cnt.HOST, cnt.PORT, db_name= cnt.PERSISTENT_DB)

    def dataAPIPersistanceLoader(self):

        collectionName = self.resourceCollectionAvgRent
        
        temporalFiles = self.db_temporal.find(collectionName)

        jsonList = []
        for doc in temporalFiles:
            print(type(doc))
            print()

        self.db.insert_many(collectionName, jsonList)

        return True

if __name__ == "__main__":

    dataLoader = dataPersistanceLoader()

    dataLoader.dataAPIPersistanceLoader()