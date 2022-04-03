# Custom class to connect and interact with the Mongo data base
from MongoDB import MongoDB

# Custom classes for both phases
from dataCollector import dataCollector
from dataPersistenceLoader import dataPersistenceLoader

if __name__ == "__main__":

	host = '10.4.41.45'
	port = 27017

	# ---------------
	# DATA COLLECTORS
	# ---------------

	p1_landing_db_name = 'p1_landing_db'
	p1_landing_db = MongoDB(host, port, p1_landing_db_name)

	# Instantiate the data collector
	dc = dataCollector()

	# Import idealista dataset
	idealista_path = 'C:\\Users\\Albert Pita\\Desktop\\BDM\\sources\\idealista'
	idealista_name = 'idealista'
	dc.import_dataset(p1_landing_db, idealista_path, idealista_name)

	# Import opendatabcn income dataset
	opendatabcn_income_path = 'C:\\Users\\Albert Pita\\Desktop\\BDM\\sources\\opendatabcn-income'
	opendatabcn_income_name = 'opendatabcn_income'
	dc.import_dataset(p1_landing_db, opendatabcn_income_path, opendatabcn_income_name)

	# Import the opendatabcn average monthly rent dataset through api
	opendatabcn_url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?'
	opendatabcn_avg_rent_name = 'opendatabcn_avg_rent'
	resource_dict_avg_rent = {
		'2014': '5855ba6c-f554-4a99-837a-04ea69bc71f4',
		'2015': 'fcdbfa43-d97a-4da3-b78b-6f255dbcf4cc',
		'2016': 'b45e8b56-1988-4474-bf61-0a76f8ab28c2',
		'2017': '0a71a12d-55fa-4a76-b816-4ee55f84d327',
		'2018': '3dc45b16-42a9-4f57-9863-e6d1a4f5869f',
		'2019': '004c76b1-6269-4136-89b2-89fd47046930',
		'2020': '47c9d64d-317a-45d0-8c45-45488df8601c',
		'2021': 'cfc45f2b-62eb-4621-8486-1b90e36b4bfe'  
	}
	dc.api_import_dataset(p1_landing_db, opendatabcn_url, resource_dict_avg_rent, 32000, opendatabcn_avg_rent_name)

	# Import lookup tables
	lookup_tables_path = 'C:\\Users\\Albert Pita\\Desktop\\BDM\\sources\\lookup_tables'
	lookup_tables_name = 'lookup_tables'
	dc.import_dataset(p1_landing_db, lookup_tables_path, lookup_tables_name)

	# -----------------------
	# DATA PERSISTENCE LOADER
	# -----------------------

	p1_persistence_db_name = 'p1_persistence_db'
	p1_persistence_db = MongoDB(host, port, p1_persistence_db_name)

	# Instantiate the data persistence loader
	dpl = dataPersistenceLoader()

	# Load idealista dataset to the persistent zone
	persistent_idealista_name = 'persistent_idealista'
	dpl.persistent_load(p1_landing_db, idealista_name, p1_persistence_db, persistent_idealista_name)

	# Load opendatabcn income dataset to the persistent zone
	persistent_opendatabcn_income_name = 'persistent_opendatabcn_income'
	dpl.persistent_load(p1_landing_db, opendatabcn_income_name, p1_persistence_db, persistent_opendatabcn_income_name)

	persistence_opendatabcn_avg_rent = 'persistent_opendatabcn_avg_rent'
	dpl.api_persistent_load(p1_landing_db, opendatabcn_avg_rent_name, p1_persistence_db, persistence_opendatabcn_avg_rent)

	# Load lookup tables to the persistent zone
	persistent_lookup_tables_name = 'persistent_lookup_tables'
	dpl.persistent_load(p1_landing_db, lookup_tables_name, p1_persistence_db, persistent_lookup_tables_name)
