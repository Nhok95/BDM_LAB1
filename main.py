import pprint
from pymongo import MongoClient


client = MongoClient()

#client = MongoClient('localhost', 27017)
client = MongoClient('dtim.essi.upc.edu', 27017)


db = client.DBkleber_reyes

countries = db.country

pprint.pprint(countries.find_one())