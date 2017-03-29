import urllib.parse
import requests

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

 def convert(link):
 	"""
	Converts a URL into a DOI, PMID, or PMCID, using some URL interpreting
	strategies and using the Citoid service as a backup plan.

	@param link: string
	@return object with keys 'doi', 'pmid', and 'pmcid'
 	"""

 	link = link.replace(' ', '')

    if link.endswith('.pdf'):
        return {'doi': None, 'pmid': None, 'pmcid': None}  # Don't bother

 	doi = None
 	pmid = None
 	pmcid = None

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
            pmcid = pmcid.replace('/', '')
        elif link.startswith('http://www.ncbi.nlm.nih.gov/pmc/articles/PMC'):
            pmcid = link.replace('http://www.ncbi.nlm.nih.gov/pmc/articles/PMC', '')
            pmcid = pmcid.replace('/', '')
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

    return {'doi': doi, 'pmid': pmid, 'pmcid': pmcid}
