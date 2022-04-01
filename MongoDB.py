# Used to connect to the mongo client
from pymongo import MongoClient

class MongoDB(object):
	"""
	This class represents a Mongo data base.
	It was created in order to make it easier for us to connect and interact
	with them.
	"""

	database = None

	def __init__(self, host, port, db_name):
		client = MongoClient(host = host, port = port)
		self.database = client[db_name]

	def create_collection(self, name):
		collection_list = self.database.list_collection_names()

	def collection_exists(self, name):
		collection_list = self.database.list_collection_names()
		if name in collection_list:
			return True
		return False

	def insert_one(self, collection, data):
		inserted_id = self.database[collection].insert_one(data)
		return inserted_id

	def insert_many(self, collection, data):
		inserted_ids = self.database[collection].insert_many(data)
		return inserted_ids

	def find_one(self, collection, query = None):
		return self.database[collection].find_one(query)

	def find(self, collection, query = None, sort = False, sort_by = None, order = 1, limit = None):
		query_result = self.database[collection].find(query)

		if sort and sort_by != None and (order == 1 or order == -1):
			query_result = query_result.sort(sort_by, order)

		if limit != None and isinstance(limit, int):
			query_result = query_result.limit(limit)

		return query_result

	def count_documents(self, collection, query = {}):
		documents_count = self.database[collection].count_documents(query)
		return documents_count

	def delete_one(self, collection, query):
		self.database[collection].delete_one(query)

	def delete_many(self, collection, query):
		deleted_count = self.database[collection].delete_many(query)
		return deleted_count

	def delete_all(self, collection, query):
		deleted_count = self.database[collection].delete_many({})
		return deleted_count

	def delete_collection(self, collection):
		self.database[collection].drop

	def update_one(self, collection, query, new_values, upsert = False):
		self.database[collection].update_one(query, new_values, upsert)

	def update_many(self, collection, query, new_values, upsert = False):
		modified_count = self.database[collection].update_many(query, new_values, upsert)
		return modified_count
		
