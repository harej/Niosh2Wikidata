import arrow
import json
import os
import URLtoIdentifier
from wikidataintegrator import wdi_core, wdi_login
from wikidata_credentials import *

WIKI_SESSION = wdi_login.WDLogin(user=wikidata_username, pwd=wikidata_password)

class WikidataEntry:
    def __init__(self, nioshtic_blob, retrieved_date):
        """
        Constructor of the WikidataEntry class.

        @param nioshtic_blob: the dictionary representing one NIOSHTIC entry
        @param retrieved_date: string representing the date of data retrieval
        """
        self.wikidata_id = nioshtic_blob['Wikidata']
        self.nioshtic_blob = nioshtic_blob
        self.item_engine = wdi_core.WDItemEngine(wd_item_id=self.wikidata_id)
        self.raw = self.item_engine.wd_json_representation
        self.data = []

        self.ref = [[
            wdi_core.WDItemID(value='Q26822184', prop_nr='P248', is_reference=True),
            wdi_core.WDExternalID(nioshtic_blob['NN'], prop_nr='P2880', is_reference=True),
            wdi_core.WDTime(retrieved_date, prop_nr='P813', is_reference=True)
        ]]

    def has_property(self, prop_nr):
        """
        Does the WikidataEntry have a given property with a property ID?

        @param prop_nr: string of Wikidata property number (e.g. P31)
        @return bool
        """

        claims = self.raw['claims']

        if prop_nr in claims:
            return True

        return False

    def append(self, datatype, prop_nr, value, qualifiers=[]):
        """
        Append a statement to a WikidataEntry.

        @param datatype: string, externalid, itemid, or date
        @param prop_nr: string Wikidata property ID (e.g. P31)
        @param value: string representing the value of the statement
        @param qualifiers: list of qualifiers that are WDBaseDataType children
        """
        statement = ''

        if datatype == 'string':
            statement = wdi_core.WDString(
                            value=value,
                            prop_nr=prop_nr,
                            references=self.ref,
                            qualifiers=qualifiers)
        elif datatype == 'externalid':
            statement = wdi_core.WDExternalID(
                            value,
                            prop_nr=prop_nr,
                            references=self.ref,
                            qualifiers=qualifiers)
        elif datatype == 'itemid':
            statement = wdi_core.WDItemID(
                            value=value,
                            prop_nr=prop_nr,
                            references=self.ref,
                            qualifiers=qualifiers)
        elif datatype == 'date':  # Technically this should be "time"
            statement = wdi_core.WDTime(
                            value,
                            prop_nr=prop_nr,
                            references=self.ref,
                            qualifiers=qualifiers)
        else:
            raise ValueError('`datatype` should be string, externalid, itemid, or date')

        if statement == '':
            raise ValueError('Statement somehow not defined??')

        self.data.append(statement)

    def save(self):
        # Creating item engine anew since there is no clear way to assign data
        # to an existing one.
        item_engine = wdi_core.WDItemEngine(
            wd_item_id=self.wikidata_id,
            data=self.data)

        item_engine.write()

def fill(nioshtic_data):
    """
    Fill out several Wikidata items based on NIOSHTIC data

    @param nioshtic_data: a dictionary with lots of NIOSHTIC data
    """

    for entry in nioshtic_data['entries']:
        if 'Wikidata' in entry:
            wd = WikidataEntry(entry, nioshtic_data['retrieved'])

            # Check if there are identifiers we can add
            if wd.has_property('P356') is False \
            or wd.has_property('P698') is False \
            or wd.has_property('P932') is False \
            and entry['DT'].lower() in ['journal article', 'book']:
                identifiers = URLtoIdentifier(entry['LT'])

                if 'doi' in identifiers and wd.has_property('P356') is False:
                    wd.append('externalid', 'P356', identifiers['doi'])

                if 'pmid' in identifiers and wd.has_property('P698') is False:
                    wd.append('externalid', 'P698', identifiers['pmid'])

                if 'pmcid' in identifiers and wd.has_property('P932') is False:
                    wd.append('externalid', 'P932', identifiers['pmcid'])

            '''
            A guide to NIOSHTIC keys:
            "TI" = title, usually ending in a period
            "AU" = semicolon delineated list of authors
            "SO" = "source"; a formatted citation
            "KW" = semicolon delineated text keywords (use as main subjects)
            "CN" = semicolon delineated CAS numbers (use as main subject)
            "DP" = date of publication in format YYYYMMDD
            "NA" = NTIS accession number (get this property created!)
            "DT" = document type
            "SC" = SIC code, or NAICS code if prefixed with "NAICS-"
            "PA" = priority area, comma-delineated (use as main subject)
            "IB" = ISBN (10 or 13) for either a chapter or a book
            "Author Keywords"/"Author keywords" = see KW
            '''

            # instance of
            if wd.has_property('P31') is False:
                # ...



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
