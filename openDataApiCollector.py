import os
from os.path import join, isfile
import json
import pandas as pd
import pprint


import urllib
import urllib.request


class openDataCollector:

    sourcePath = join(os.getcwd(),'Sources')

    apiUrl = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?'
    resourceDictBirths = {
        '2007': '1b1bdb74-6078-40e8-a002-fcff87295688',
        '2008': 'f6029e43-e5f9-4623-9c23-bf0aba7dd39b',
        '2009': '',
        '2010': '',
        '2011': '',
        '2012': '',
        '2013': '',
        '2014': '',
        '2015': '',
        '2016': '',
        '2017': '',
        '2018': '091e6a7f-9454-4c31-834b-a27ad9062952',
        '2019': '83a0195f-ce6f-4bcc-9ffb-b863ab5de468',
        '2020': '6f4cbe8a-58b1-492a-aa5d-b8bbaf8a9987'
        
    }

    resourceDictAvgRent = {
        '2014': '',
        '2015': '',
        '2016': '',
        '2017': '0a71a12d-55fa-4a76-b816-4ee55f84d327',
        '2018': '',
        '2019': '',
        '2020': '',
        '2021': ''
        
    }

    def __init__(self):
        pass

    def openDataAPICollector(self):
        
        params = {
            'resource_id': '0a71a12d-55fa-4a76-b816-4ee55f84d327',
            'limit': 32000 #upper limit by default in CKAN Data API 
        }

        url = self.apiUrl + urllib.parse.urlencode(params)
        print(url)
        print()

        response = urllib.request.urlopen(url)

        data_json = json.loads(response.read())

        if data_json.get('success') == True:
            data = data_json.get('result')

        #pprint.pprint(data_json)

        
        return True




if __name__ == "__main__":

    dataCollector = openDataCollector()

    #idealista = dataExtractor.idealistaDataExtractor()
    api = dataCollector.openDataAPICollector()

    #print(len(idealista))
    #print(len(lookUp))