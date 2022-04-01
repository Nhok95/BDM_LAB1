# Custom class to connect and interact with the Mongo data base
from MongoDB import MongoDB

# Used to print the output clearear and prettier
from pprint import pprint

import os
import shutil
import re
from datetime import date

import json
import pandas as pd

def csv_to_json(csv_path, new_json_path):

	df = pd.read_csv(csv_path)
	df.to_json(new_json_path)

def check_for_date(filename):

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

def import_dataset(mongodb, local_directory, dataset_name):

	# Check if the collection already exists
	# if not, create a new one in the db
	collection_name = dataset_name
	if not mongodb.collection_exists(collection_name):
		mongodb.create_collection(collection_name)

	# Check if the folder processed exists inside the folder
	# the dataset
	processed_path = os.path.join(local_directory, 'processed')
	if not os.path.exists(processed_path):
		os.mkdir(processed_path)

	# For each file inside the dataset its extension is checked
	# if its a json the file is directly inserted in the collection
	# if its a csv first its converted to json and then inserted
	for filename in os.listdir(local_directory):

		f = os.path.join(local_directory, filename)
		if os.path.isfile(f):

			# The variable json_path indicates the path of the final
			# json to insert in the database (processed or not)
			if f.endswith('json'):
				json_path = f
				json_name = filename

			elif f.endswith('.csv'):
				json_name = filename.replace('.csv', '.json')
				json_path = os.path.join(local_directory, json_name)
				csv_to_json(f, json_path)

				processed_csv = os.path.join(processed_path, filename)
				shutil.move(f, processed_csv)

			else:
				print('The file {0} was not processed as is not a json nor a csv.'.format(filename))
				break

			# Read the final json
			with open(json_path, 'r') as f_json:
				f_content = json.load(f_json)

			# Check if it is a list of documents or 
			# just one document alone, and also check
			# if the list is empty (mongo do not accept inserts
			# with empty ones)
			if isinstance(f_content, list):
				if f_content:

					for i, _ in enumerate(f_content):
						f_content[i]["origin_filename"] = json_name

					mongodb.insert_many(collection_name, f_content)
				else:
					print('The file {0} is empty.'.format(json_name))
			else:
				f_content['origin_filename'] = json_name
				mongodb.insert_one(collection_name, f_content)

			# Move the file already processed to the processed folder
			processed_json = os.path.join(processed_path, json_name)
			shutil.move(json_path, processed_json)

def add_metadata(document):

	# Check if there is a date in the filename
	# if not, add the current one
	date = check_for_date(document['origin_filename'])

	metadata = {
		'insertion_date': date,
	}
	for key, value in document.items():
		metadata[key] = str(type(value))

	document["metadata"] = metadata

	return document

def insert_batch(batch, persistent_db, target_collection):

	for document in batch:

		# Read the document adding metadata
		document_with_metadata = add_metadata(document)

		# Add the documents to the persistent_db
		persistent_db.update_one(target_collection, document_with_metadata, upsert = True)

def delete_batch(source_db, batch, source_collection):

	for document in batch:

		# Delete the document from the landing db
		source_db.delete_one(source_collection, document)

def move_documents(source_db, source_collection, persistent_db, target_collection, batch_size):

	documents_count = source_db.count_documents(source_collection)
	print('Moving ' + str(documents_count) + ' documents from ' + source_collection + '.')

	while documents_count > 0:

		batch_documents = source_db.find(source_collection, limit = batch_size)

		insert_batch(batch_documents, persistent_db, target_collection)
		delete_batch(source_db, batch_documents, source_collection)

		documents_count = source_db.count_documents(source_collection)
		print(str(documents_count) + ' remaining to move.')

def persistent_load(landing_db, temporal_collection, persistent_db, persistent_collection):

	# Check if the persistent collection already exists
	# if not, create a new one in the persistentdb
	if not persistent_db.collection_exists(persistent_collection):
		persistent_db.create_collection(persistent_collection)

	move_documents(landing_db, temporal_collection, persistent_db, persistent_collection, 5)

	# Delete the temporal collection if empty
	# Normally it should always occur
	if landing_db.count_documents(temporal_collection) == 0:
		landing_db.delete_collection(temporal_collection)

if __name__ == "__main__":

	host = '10.4.41.45'
	port = 27017

	# ---------------
	# DATA COLLECTORS
	# ---------------

	p1_landing_db_name = 'p1_landing_db'
	p1_landing_db = MongoDB(host, port, p1_landing_db_name)

	# Import idealista dataset
	idealista_path = 'C:\\Users\\Albert Pita\\Desktop\\BDM\\sources\\idealista'
	idealista_name = 'idealista'
	import_dataset(p1_landing_db, idealista_path, idealista_name)

	# Import opendatabcn income dataset
	opendatabcn_income_path = 'C:\\Users\\Albert Pita\\Desktop\\BDM\\sources\\opendatabcn-income'
	opendatabcn_income_name = 'opendatabcn_income'
	import_dataset(p1_landing_db, opendatabcn_income_path, opendatabcn_income_name)

	# Import lookup tables
	lookup_tables_path = 'C:\\Users\\Albert Pita\\Desktop\\BDM\\sources\\lookup_tables'
	lookup_tables_name = 'lookup_tables'
	import_dataset(p1_landing_db, lookup_tables_path, lookup_tables_name)

	# -----------------------
	# DATA PERSISTENCE LOADER
	# -----------------------

	p1_persistence_db_name = 'p1_persistence_db'
	p1_persistence_db = MongoDB(host, port, p1_persistence_db_name)

	# Load idealista dataset to the persistent zone
	persistent_idealista_name = 'persistent_idealista'
	persistent_load(p1_landing_db, idealista_name, p1_persistence_db, persistent_idealista_name)

	# Load opendatabcn income dataset to the persistent zone
	persistent_opendatabcn_income_name = 'persistent_opendatabcn_income'
	persistent_load(p1_landing_db, opendatabcn_income_name, p1_persistence_db, persistent_opendatabcn_income_name)

	# Load lookup tables to the persistent zone
	persistent_lookup_tables_name = 'persistent_lookup_tables'
	persistent_load(p1_landing_db, lookup_tables_name, p1_persistence_db, persistent_lookup_tables_name)
