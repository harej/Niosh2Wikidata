import arrow
import json
import os
from wikidataintegrator import wdi_core, wdi_login
from wikidata_credentials import *

WIKI_SESSION = wdi_login.WDLogin(user=wikidata_username, pwd=wikidata_password)

class WikidataEntry:
	def __init__(self, wikidata_id, nioshtic_blob, retrieved_date):
		self.wikidata_id = wikidata_id
		self.nioshtic_blob = nioshtic_blob
		self.item_engine = wdi_core.WDItemEngine(wd_item_id=wikidata_id)
		self.data = []

		self.ref = [[
            wdi_core.WDItemID(value='Q26822184', prop_nr='P248', is_reference=True),
            wdi_core.WDExternalID(nioshtic_blob['NN'], prop_nr='P2880', is_reference=True),
            wdi_core.WDTime(retrieved_date, prop_nr='P813', is_reference=True)
        ]]

    def append(self, datatype, prop_nr, value):
    	if datatype == 'string':
    		statement = wdi_core.WDString(
    						value=value,
    						prop_nr=prop_nr,
    						references=self.ref)


	def save(self):
		# Creating item engine anew since there is no clear way to assign data
		# to an existing one.
		item_engine = wdi_core.WDItemEngine(
			wd_item_id=self.wikidata_id,
			data=self.data)

		item_engine.write()



def fill(nioshtic_data):
	# Instantiate the WikidataEntry using the Wikidata ID number

def process_file(filename):
    """
    Loads a JSON file and runs it through the item create/edit methods.

    @param filename: name of file to process (e.g. output.txt.json)
    """

    with open(filename) as f:
        nioshtic_data = json.load(f)
        fill(nioshtic_data)
        print("Processed: " + filename)

def main():
    """
    If this file is invoked from command line, autodiscover JSON blobs in the
    raw/ subdirectory and process them.
    """

    for filename in os.listdir('raw/'):
        if filename.lower().endswith('.json'):
            process_file('raw/' + filename)

if __name__ == '__main__':
	main()
