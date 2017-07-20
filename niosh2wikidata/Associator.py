#!/usr/local/bin/python3
"""
Takes JSON output from Cleaner and associates it with other identifiers for
cross-referencing.
"""

import json
import os
import requests


def get_nioshtic_wikidata_mapping():
    """
    Retrieves a mapping between NIOSHTIC and Wikidata identifiers from the
    Wikidata Query Service, query.wikidata.org

    @return dictionary {nioshtic: {identifier_label: value}}
    """

    prefix = 'http://www.wikidata.org/entity/'
    q = (
        'select%20%3Fi%20%3Fn%20%3Fdoi%20%3Fpubmed%20%3Fpmcid%20%3Fisbn10%20'
        '%3Fisbn13%20where%20%7B%3Fi%20wdt%3AP2880%20%3Fn%20.%20optional%20%7B'
        '%20%3Fi%20wdt%3AP356%20%3Fdoi%20%7D%20.%20optional%20%7B%20%3Fi%20wdt'
        '%3AP698%20%3Fpubmed%20%7D%20.%20optional%20%7B%20%3Fi%20wdt%3AP932%20'
        '%3Fpmcid%20%7D%20.%20optional%20%7B%20%3Fi%20wdt%3AP957%20%3Fisbn10'
        '%20%7D%20.%20optional%20%7B%20%3Fi%20wdt%3AP212%20%3Fisbn13%20%7D%20'
        '%7D')
    url = 'https://query.wikidata.org/sparql?format=json&query=' + q

    try:
        query = requests.get(url).json()['results']['bindings']
    except:
        raise Exception("Wikidata query not possible. Try again later.")

    data = {}

    for x in query:
        key = x['n']['value']
        data[key] = {'Wikidata': x['i']['value'].replace(prefix, '')}

        if 'doi' in x:
            data[key]['DOI'] = x['doi']['value']

        if 'pubmed' in x:
            data[key]['PubMed ID'] = x['pubmed']['value']

        if 'pmcid' in x:
            data[key]['PMCID'] = x['pmcid']['value']

        if 'isbn10' in x:
            data[key]['ISBN-10'] = x['isbn10']['value']

        if 'isbn13' in x:
            data[key]['ISBN-13'] = x['isbn13']['value']

    return data


def add_wikidata(nioshtic_data):
    """
    Associates NIOSHTIC entries with equivalent Wikidata identifiers, or skips
    over if there is no Wikidata identifier.

    This only maps if the NIOSHTIC identifier is in fact used on Wikidata. It
    does not cover, e.g. a NIOSH journal article with a given PubMed ID that is
    on Wikidata but does not have the associated NIOSHTIC identifier as well.
    This is handled at a later step.

    This also checks for PubMed ID, PMCID, DOI, ISBN-10, and ISBN-13.

    @param nioshtic_data dictionary with "entries" and "headers" keys
    @return new, updated dictionary
    """

    if 'headers' not in nioshtic_data or 'entries' not in nioshtic_data:
        raise ValueError('Data dictionary must have headers and entries keys')

    n_to_wd = get_nioshtic_wikidata_mapping()
    c = 0

    for entry in nioshtic_data['entries']:
        if 'NN' not in entry:
            continue
        if entry['NN'] in n_to_wd.keys():
            for header, val in n_to_wd[entry['NN']].items():
                nioshtic_data['entries'][c][header] = val

                if header not in nioshtic_data['headers']:
                    nioshtic_data['headers'].append(header)
        c += 1

    return nioshtic_data


def process_file(filename):
    """
    Loads a JSON file, runs it through the associator methods, and replaces the
    old file.

    @param filename: name of file to process (e.g. output.txt.json)
    @return new file (e.g. output.txt.json)
    """

    with open(filename, 'r+') as f:
        nioshtic_data = json.load(f)
        nioshtic_data = add_wikidata(nioshtic_data)
        f.seek(0)
        json.dump(nioshtic_data, f, indent=4)
        f.truncate()

        print("Updated file saved to: " + filename)


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
