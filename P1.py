# Custom class to connect and interact with the Mongo data base
from MongoDB import MongoDB

# Used to print the output clearear and prettier
from pprint import pprint

import os
import shutil

import json
import pandas as pd

def csv_to_json(csv_path, new_json_path):

	df = pd.read_csv(csv_path)
	df.to_json(new_json_path)

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
					mongodb.insert_many(collection_name, f_content)
				else:
					print('The file {0} is empty.'.format(json_name))
			else:
				mongodb.insert_one(collection_name, f_content)

			# Move the file already processed to the processed folder
			processed_json = os.path.join(processed_path, json_name)
			shutil.move(json_path, processed_json)

if __name__ == "__main__":

	host = '10.4.41.45'
	port = 27017
	db_name = 'p1_db'

	p1_db = MongoDB(host, port, db_name)

	# Import idealista dataset
	idealista_path = 'C:\\Users\\Albert Pita\\Desktop\\BDM\\sources\\idealista'
	idealista_name = 'idealista'
	import_dataset(p1_db, idealista_path, idealista_name)

	# Import opendatabcn income dataset
	opendatabcn_income_path = 'C:\\Users\\Albert Pita\\Desktop\\BDM\\sources\\opendatabcn-income'
	opendatabcn_income_name = 'opendatabcn_income'
	import_dataset(p1_db, opendatabcn_income_path, opendatabcn_income_name)

	# Import lookup tables
	lookup_tables_path = 'C:\\Users\\Albert Pita\\Desktop\\BDM\\sources\\lookup_tables'
	lookup_tables_name = 'lookup_tables'
	import_dataset(p1_db, lookup_tables_path, lookup_tables_name)
