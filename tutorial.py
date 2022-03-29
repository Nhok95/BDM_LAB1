import pprint
from pymongo import MongoClient


client = MongoClient('localhost', 27017)


db = client.DBkleber_reyes

countries = db.country

pprint.pprint(countries.find_one())