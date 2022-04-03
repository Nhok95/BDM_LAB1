# Custom class to connect and interact with the Mongo data base
from MongoDB import MongoDB

import re
from datetime import date

class dataPersistenceLoader:

	def __init__(self):
		pass

	# ---------------
	# Private methods
	# ---------------

	def __check_for_date(self, filename):

		s_numbers = re.findall('[0-9]+', filename)

		# If there are no numbers inside the filename
		# we use the actual date as the insertion time
		if len(s_numbers) == 0:
			today = date.today().strftime('%Y-%m-%d')
			return today

		# If there are numbers we assume they are ordered as
		# YYYY - MM - DD - (if others do not care)
		# Default values and conditions to check date could
		# be improved
		else:
			n_numbers = len(s_numbers)
			
			year = '1999'
			if n_numbers >= 1 and len(s_numbers) == 4:
				year = s_numbers[0]

			month = '01'
			if n_numbers >= 2 and len(s_numbers) == 2:
				month = s_numbers[1]

			day = '01'
			if n_numbers >= 3 and len(s_numbers) == 2:
				day = s_numbers[2] 

			return year + '-' + month + '-' + day

	def __insert_batch(self, batch, persistent_db, target_collection):

		inserted_ids = []
		for document in batch:

			# Change document estructure
			doc_id = document['_id']

			if 'data' in document.keys():
				data = {}
				data['records'] = document['data']
				data['origin_filename'] = document['origin_filename']
			else:
				# Select all attributes except id
				data = { k:document[k] for k in document.keys() if k != '_id' }

			metadata = {}
			metadata['origin_date'] = self.__check_for_date(document['origin_filename'])
			metadata['ingested_time'] = date.today().strftime('%Y-%m-%d')
			if 'schema' in document.keys():
				schema = {}
				for field in document['schema']['fields']:
					id_str = field['name']
					type_str = field['type']

					schema[id_str] = type_str

				metadata['schema'] = schema
			else:
				for key, value in document.items():
					metadata[key] = str(type(value))

			# Final structure
			updated_document = {
				"value": {
					"data": data,
					"metadata": metadata
				}
			}

			# Add the documents to the persistent_db
			persistent_db.insert_one(target_collection, updated_document)
			inserted_ids.append(doc_id)

		return inserted_ids

	def __insert_api_batch(self, batch, persistent_db, target_collection):

		inserted_ids = []
		for document in batch:

			# Change document estructure
			doc_id = document['_id']

			data = {}
			data['records'] = document['records']
			data['origin_filename'] = document['origin_filename']

			metadata = {}
			metadata['origin_date'] = self.__check_for_date(document['origin_filename'])
			metadata['ingested_time'] = date.today().strftime('%Y-%m-%d')

			schema = {}
			for field in document['fields']:
				id_str = field['id']
				type_str = field['type']

				schema[id_str] = type_str
			
			metadata['schema'] = schema

			# Final structure
			updated_document = {
				"value": {
					"data": data,
					"metadata": metadata
				}
			}

			# Add the documents to the persistent_db
			persistent_db.insert_one(target_collection, updated_document)

			inserted_ids.append(doc_id)

		return inserted_ids

	def __delete_batch(self, source_db, to_delete_ids, source_collection):

		for to_delete_id in to_delete_ids:

			# Delete the document from the landing db
			source_db.delete_one(source_collection, {'_id': to_delete_id})

	def __move_documents(self, source_db, source_collection, persistent_db, target_collection, batch_size):

		documents_count = source_db.count_documents(source_collection)
		print('Moving ' + str(documents_count) + ' documents from ' + source_collection + '.')

		while documents_count > 0:

			batch_documents = source_db.find(source_collection, limit = batch_size)

			inserted_ids = self.__insert_batch(batch_documents, persistent_db, target_collection)
			self.__delete_batch(source_db, inserted_ids, source_collection)

			documents_count = source_db.count_documents(source_collection)
			print(str(documents_count) + ' remaining to move.')

	def __move_api_documents(self, source_db, source_collection, persistent_db, target_collection, batch_size):

		documents_count = source_db.count_documents(source_collection)
		print('Moving ' + str(documents_count) + ' documents from ' + source_collection + '.')

		while documents_count > 0:

			batch_documents = source_db.find(source_collection, limit = batch_size)

			inserted_ids = self.__insert_api_batch(batch_documents, persistent_db, target_collection)
			self.__delete_batch(source_db, inserted_ids, source_collection)

			documents_count = source_db.count_documents(source_collection)
			print(str(documents_count) + ' remaining to move.')

	# ---------------
	# Public methods
	# ---------------

	def persistent_load(self, landing_db, temporal_collection, persistent_db, persistent_collection):

		print('Loading to the persistent zone ' + persistent_collection + \
			' the collection ' + temporal_collection)

		# Check if the persistent collection already exists
		# if not, create a new one in the persistentdb
		if not persistent_db.collection_exists(persistent_collection):
			persistent_db.create_collection(persistent_collection)

		self.__move_documents(landing_db, temporal_collection, persistent_db, persistent_collection, 100)

		# Delete the temporal collection if empty
		# Normally it should always occur
		if landing_db.count_documents(temporal_collection) == 0:
			landing_db.delete_collection(temporal_collection)

	def api_persistent_load(self, landing_db, temporal_collection, persistent_db, persistent_collection):

		print('Loading to the persistent zone ' + persistent_collection + \
			' the collection ' + temporal_collection)

		# Check if the persistent collection already exists
		# if not, create a new one in the persistentdb
		if not persistent_db.collection_exists(persistent_collection):
			persistent_db.create_collection(persistent_collection)

		self.__move_api_documents(landing_db, temporal_collection, persistent_db, persistent_collection, 100)

		# Delete the temporal collection if empty
		# Normally it should always occur
		if landing_db.count_documents(temporal_collection) == 0:
			landing_db.delete_collection(temporal_collection)
