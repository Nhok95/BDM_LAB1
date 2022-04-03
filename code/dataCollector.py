# Custom class to connect and interact with the Mongo data base
from MongoDB import MongoDB

import os
import shutil

import json
import pandas as pd

import urllib
import urllib.request

class dataCollector:

	def __init__(self):
		pass

	# ---------------
	# Private methods
	# ---------------

	def __csv_to_json(self, csv_path, new_json_path):

		df = pd.read_csv(csv_path)
		df.to_json(new_json_path)

	# ---------------
	# Public methods
	# ---------------

	def import_dataset(self, mongodb, local_directory, dataset_name):

		# Check if the collection already exists
		# if not, create a new one in the db
		collection_name = dataset_name

		print('Checking if ' + collection_name + ' exists.')
		if not mongodb.collection_exists(collection_name):
			print('It do not exists. Creating it.')
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
			print('Processing the file ' + filename)

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

					print('Converting the csv to a json file.')
					self.__csv_to_json(f, json_path)

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

	def api_import_dataset(self, mongodb, url, resources, limit, dataset_name):

		# Check if the collection already exists
		# if not, create a new one in the db
		collection_name = dataset_name

		print('Checking if ' + collection_name + ' exists.')
		if not mongodb.collection_exists(collection_name):
			print('It do not exists. Creating it.')
			mongodb.create_collection(collection_name)

		print('Processing data from the url ' + url + \
			' the dataset ' + dataset_name)

		for resource_year, resource_id in resources.items():

			print('Resource year: ' + resource_year)

			params = {
				'resource_id': resource_id,
				'limit': limit # upper limit by default in CKAN Data API 
			}

			complete_url = url + urllib.parse.urlencode(params)

			response = urllib.request.urlopen(complete_url)
			data_json = json.loads(response.read())

			if data_json.get('success') == True:

				f_content = data_json.get('result')
				f_content['origin_filename'] = resource_year + '_' + collection_name

				mongodb.insert_one(collection_name, f_content)
