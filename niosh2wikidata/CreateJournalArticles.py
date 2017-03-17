#!/usr/local/bin/python3.6

"""
The first pass at importing data to Wikidata.

Journal articles are definitely eligible to be included on Wikidata, and there
is plenty of rich metadata to pick from, so we can just iterate through each
entry and do it totally automatic.
"""

import json
import os
import requests
import urllib.parse
from wikidataintegrator import wdi_core, wdi_login
from wikidata_credentials import *
from pprint import pprint

try:
    from libs.BiblioWikidata import JournalArticles
except ImportError:
    raise ImportError('Did you remember to `git submodule init` '
                      'and `git submodule update`?')

WIKI_SESSION = wdi_login.WDLogin(user=wikidata_username, pwd=wikidata_password)

def append_identifiers(wikidata_id, doi=None, pmid=None, pmcid=None,
                       nioshtic=None):
    """
    Adds identifiers such as DOI and NIOSHTIC to an existing Wikidata item.
    Reconciliation of identifiers across databases helps us root out duplicates.

    @param wikidata_id: the Q-number of the Wikidata item to edit
    @param doi: string; defaults to None
    @param pmid: string; defaults to None
    @param pmcid: string; defaults to None
    @param nioshtic: string; defaults to None
    """
    data = []
    if doi is not None:
        to_append = wdi_core.WDString(value=doi, prop_nr='P356')
        data.append(to_append)
    if pmid is not None:
        to_append = wdi_core.WDString(value=pmid, prop_nr='P698')
        data.append(to_append)
    if pmcid is not None:
        to_append = wdi_core.WDString(value=pmcid, prop_nr='P932')
        data.append(to_append)
    if nioshtic is not None:
        to_append = wdi_core.WDString(value=nioshtic, prop_nr='P2880')
        data.append(to_append)

    append_value = ['P356', 'P698', 'P932', 'P2880']
    wikidata_item = wdi_core.WDItemEngine(wd_item_id=wikidata_id, data=data,
                                          append_value=append_value)
    wikidata_item.write(WIKI_SESSION)

    if doi is None:
        doi = ''
    if pmid is None:
        pmid = ''
    if pmcid is None:
        pmcid = ''
    if nioshtic is None:
        nioshtic = ''
    print(wikidata_id + '|' + doi + '|' + pmid + '|' + pmcid + '|' + nioshtic)

def get_citoid(to_lookup):
    """
    Does a lookup to the Wikimedia Citoid instance.

    @param to_lookup: string URL to look up
    @return dictionary representing results
    """

    url = "https://en.wikipedia.org/api/rest_v1/data/citation/mediawiki/"
    url += urllib.parse.quote_plus(to_lookup)

    try:
        query = requests.get(url).json()
    except:
        raise Exception(url)

    return query

def get_mapping(wd_prop):
    """
    Meta-method for the three below. Takes a Wikidata property number (P123)
    and does the lookup that returns the mapping.

    @param wd_prop: string representing Wikidata property number e.g. P356.
    @return dictionary {identifier: wikidata_id}
    """

    prefix = 'http://www.wikidata.org/entity/'
    q = 'select%20%3Fi%20%3Fn%20where%20%7B%20%3Fi%20wdt%3A{0}%20%3Fn%20%7D'
    q = q.format(wd_prop)
    url = 'https://query.wikidata.org/sparql?format=json&query=' + q

    try:
        query = requests.get(url).json()['results']['bindings']
    except:
        raise Exception(url)

    package = {}
    for x in query:
        if x['n']['value'] not in package:
            package[x['n']['value']] = []
        package[x['n']['value']].append(x['i']['value'].replace(prefix, ''))

    return package

def get_doi_to_wikidata():
    """
    Returns a mapping of Wikidata IDs and DOIs.
    """

    return get_mapping('P356')

def get_pmid_to_wikidata():
    """
    Returns a mapping of Wikidata IDs and PMIDs.
    """

    return get_mapping('P698')

def get_pmcid_to_wikidata():
    """
    Returns a mapping of Wikidata IDs and PMCIDs.
    """

    return get_mapping('P932')

def process_data(nioshtic_data):
    """
    The main method that kicks off the Wikidata editing. Takes a big bunch of
    data and goes through it.

    This does not handle the data within the NIOSHTIC columns themselves. It
    just looks for identifiers and then uses those identifiers to make API calls
    to the appropriate databases. The integration of NIOSHTIC content itself is
    handled through a separate class.

    @param nioshtic_data: dictionary with "entries" and "headers" keys
    """

    doi_to_wikidata = get_doi_to_wikidata()
    pmid_to_wikidata = get_pmid_to_wikidata()
    pmcid_to_wikidata = get_pmcid_to_wikidata()

    for entry in nioshtic_data['entries']:
        # If these values are populated, they were populated via the Wikidata
        # item as identified via the NIOSHTIC ID, meaning the NIOSHTIC ID is
        # already there and there is already an item filled out.
        if 'DOI' in entry \
        or 'PubMed ID' in entry \
        or 'PMCID' in entry \
        or 'Wikidata' in entry \
        or 'LT' not in entry:
            continue

        wikidata_id = []

        # The "interesting" factor: When the Wikidata item is known but has none
        # of those other identifiers, yet Citoid turns out a result anyway.
        # Meaning that the item is missing a non-NIOSHTIC identifier.
        interesting = False
        if 'Wikidata' in entry \
        and entry['DT'].lower() not in ['chapter', 'abstract']:
            wikidata_id.append(entry['Wikidata'])
            interesting = True

        doi = None
        pmid = None
        pmcid = None
        link = entry['LT'].replace(' ', '')
        if link.endswith('.pdf'):
            continue  # Don't bother
        if link.startswith('http://dx.doi.org/'):
            doi = link.replace('http://dx.doi.org/', '').upper()
        elif link.startswith('http://doi.org/'):
            doi = link.replace('http://doi.org/', '').upper()
        elif link.startswith('https://dx.doi.org/'):
            doi = link.replace('https://dx.doi.org/', '').upper()
        elif link.startswith('https://doi.org/'):
            doi = link.replace('https://doi.org/', '').upper()
        elif link.startswith('https://www.ncbi.nlm.nih.gov/pubmed/?term='):
            pmid = link.replace('https://www.ncbi.nlm.nih.gov/pubmed/?term=', '')
        elif link.startswith('http://www.ncbi.nlm.nih.gov/pubmed/?term='):
            pmid = link.replace('http://www.ncbi.nlm.nih.gov/pubmed/?term=', '')
        elif link.startswith('https://www.ncbi.nlm.nih.gov/pmc/articles/PMC'):
            pmcid = link.replace('https://www.ncbi.nlm.nih.gov/pmc/articles/PMC', '')
        elif link.startswith('http://www.ncbi.nlm.nih.gov/pmc/articles/PMC'):
            pmcid = link.replace('http://www.ncbi.nlm.nih.gov/pmc/articles/PMC', '')
        else:
            # Citoid is used as a last resort because it's super-slow.
            citoid = get_citoid(link)

            if len(citoid) != 1:
                continue

            if 'DOI' in citoid:
                doi = citoid['DOI'].upper()

            if 'PMID' in citoid:
                pmid = citoid['PMID']

            if 'PMCID' in citoid:
                pmcid = citoid['PMCID'].replace('PMC', '')

        if doi is not None and doi in doi_to_wikidata:
            for single_wikidata_id in doi_to_wikidata[doi]:
                wikidata_id.append(single_wikidata_id)

        if pmid is not None and pmid in pmid_to_wikidata:
            for single_wikidata_id in pmid_to_wikidata[pmid]:
                wikidata_id.append(single_wikidata_id)

        if pmcid is not None and pmcid in pmcid_to_wikidata:
            for single_wikidata_id in pmcid_to_wikidata[pmcid]:
                wikidata_id.append(single_wikidata_id)

        if interesting == True \
        and (doi is not None or pmid is not None or pmcid is not None):
            # wikidata_id must be defined as well
            for single_wikidata_id in wikidata_id:
                append_identifiers(single_wikidata_id, doi=doi, pmid=pmid,
                                   pmcid=pmcid)

        else:
            if wikidata_id == []:
                # No Wikidata ID was found amongst the identifiers. This means
                # the item truly does not exist, best we can tell.

                if doi is not None or pmid is not None or pmcid is not None:
                    add_data = [wdi_core.WDItemID(value='Q60346', prop_nr='P859')]
                    if entry['DT'].lower() not in ['abstract', 'book', 'chapter']:
                        add_data.append(wdi_core.WDString(value=entry['NN'], prop_nr='P2880'))
                    JournalArticles.item_creator([{'doi': doi, 'pmcid': pmcid,
                                                  'pmid': pmid, 'data': add_data}])

                    # If entry['DT'] is Abstract or Chapter, the item on that
                    # thing will be created separately from its container. 

            else:
                # Citoid found a DOI/PMID/PMCID that matched with an existing
                # Wikidata entry, which means the Wikidata entry exists but just
                # has no assigned NIOSHTIC-ID.
                if entry['DT'].lower() not in ['abstract', 'book', 'chapter']:
                    for single_wikidata_id in wikidata_id:
                        append_identifiers(single_wikidata_id, nioshtic=entry['NN'])

def process_file(filename):
    """
    Loads a JSON file and runs it through the item create/edit methods.

    @param filename: name of file to process (e.g. output.txt.json)
    """

    with open(filename) as f:
        nioshtic_data = json.load(f)
        process_data(nioshtic_data)
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
