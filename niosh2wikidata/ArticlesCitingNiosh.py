#!/usr/local/bin/python3.6

import requests

try:
    from libs.BiblioWikidata import JournalArticles
except ImportError:
    raise ImportError('Did you remember to `git submodule init` '
                      'and `git submodule update`?')

def main():
    seed = ('https://query.wikidata.org/sparql'
            '?format=json'
            '&query=select%20%3Fpmcid%20%3Fnioshtic%20where%20%7B%20'
            '%3Fi%20wdt%3AP932%20%3Fpmcid%20.%20optional%20%7B%20'
            '%3Fi%20wdt%3AP2880%20%3Fnioshtic%20.%20%7D%20%7D%20')
    pmc_template = ('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi'
                    '?dbfrom=pmc'
                    '&linkname=pmc_pmc_citedby'
                    '&tool=wikidata_worker'
                    '&email=jamesmhare@gmail.com'
                    '&retmode=json')

    get_pmcid_list = requests.get(seed)
    try:
        pmcid_blob = get_pmcid_list.json()
    except:
        print(get_pmcid_list.text)
        return

    pmcid_blob = pmcid_blob['results']['bindings']

    # Two lists: one of all the PMCIDs on Wikidata, and one of just those that
    # are from NIOSH.
    # The purpose of the first list is to see if we need to create a Wikidata
    # entry.
    # The purpose of the second is to actually send through Eutils – we are only
    # interested in a subset.

    total_pmcid_list = []
    niosh_pmcid_list = []

    for result in pmcid_blob:
        total_pmcid_list.append(result['pmcid']['value'])
        if 'nioshtic' in result:
            niosh_pmcid_list.append(result['pmcid']['value'])

    # Removing duplicates
    total_pmcid_list = list(set(total_pmcid_list))
    niosh_pmcid_list = list(set(niosh_pmcid_list))

    niosh_pmcid_list.sort(reverse=True)
    packages = [niosh_pmcid_list[x:x+200] \
                for x in range(0, len(niosh_pmcid_list), 200)]
    nonexistent = []

    for package in packages:
        query_string = ''
        for item in package:
            query_string += '&id=' + item

        r = requests.get(pmc_template + query_string)
        blob = r.json()


        for result in blob['linksets']:
            if 'linksetdbs' in result:
                for citing_id in result['linksetdbs'][0]['links']:
                    if str(citing_id) not in total_pmcid_list:
                        if str(citing_id) not in nonexistent:
                            nonexistent.append(str(citing_id))

                            JournalArticles.item_creator([{
                                'doi': None,
                                'pmcid': str(citing_id),
                                'pmid': None,
                                'data': []}])

if __name__ == '__main__':
    main()
