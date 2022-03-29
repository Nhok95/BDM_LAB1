import pprint
from pymongo import MongoClient


client = MongoClient()

client = MongoClient('localhost', 27017)

db = client.test

collection = db.CollectionName

pprint.pprint(collection.find_one())