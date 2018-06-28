import arrow
import json
import os
import re
import requests
import time
import URLtoIdentifier
from edit_queue import EditQueue
from wikidataintegrator import wdi_core, wdi_login
from wikidata_credentials import *

try:
    from libs.BiblioWikidata import JournalArticles
except ImportError:
    raise ImportError('Did you remember to `git submodule init` '
                      'and `git submodule update`?')

eq = EditQueue()

WIKI_SESSION = wdi_login.WDLogin(user=wikidata_username, pwd=wikidata_password)

isbn_map_raw = requests.get(
    'https://query.wikidata.org/sparql?format=json&query=SELECT%20%3Fitem%20%3Fisbn13%20%3Fisbn10%20WHERE%20%7B%0A%7B%0A%20%20%20%20%3Fitem%20wdt%3AP212%20%3Fisbn13%20.%0A%7D%20UNION%20%7B%0A%20%20%20%20%3Fitem%20wdt%3AP957%20%3Fisbn10%20.%0A%7D%0A%7D'
)
isbn_map_raw = isbn_map_raw.json()['results']['bindings']
isbn_map = {}

for block in isbn_map_raw:
    wd_item = block['item']['value'].replace('http://www.wikidata.org/entity/',
                                             '')
    if 'isbn13' in block:
        res = block['isbn13']['value'].replace('-', '')
    elif 'isbn10' in block:
        res = block['isbn10']['value'].replace('-', '')
    isbn_map[res] = wd_item

cas_map = requests.get(
    'https://query.wikidata.org/sparql?format=json&query=select%20%3Fi%20%3Fcas%20where%20%7B%20%3Fi%20wdt%3AP231%20%3Fcas%20%7D'
)
cas_map = cas_map.json()['results']['bindings']
cas_map = {
    x['cas']['value']:
    x['i']['value'].replace('http://www.wikidata.org/entity/', '')
    for x in cas_map
}


def indirect_identifier(wd, id_name, id_value):
    """
    Helper function to indirectly associate a WikidataEntry with another Wikidata
    entry via the "part of" property.

    @param wd: a WikidataEntry object
    @param id_name: 'doi', 'pmid', or 'pmcid'
    @param id_value: the identifier of the other work
    """

    id_entity = {'doi': 'P356', 'pmid': 'P698', 'pmcid': 'P932'}
    lookup = 'select%20%3Fi%20where%20%7B%20%3Fi%20wdt%3A{0}%20%22{1}%22%20%7D'
    lookup = lookup.format(id_entity[id_name], id_value)
    lookup = 'https://query.wikidata.org/sparql?format=json&query=' + lookup
    try:
        lookup = requests.get(lookup).json()['results']['bindings']
    except Exception as e:
        print(e)
        return

    if len(lookup) > 0:
        for result in lookup:
            relevant_item = result['i']['value'].replace(
                'http://www.wikidata.org/entity/', '')
            wd.append('itemid', 'P361', relevant_item)
    else:
        for new_item in JournalArticles.item_creator([{'doi': id_value}]):
            wd.append('itemid', 'P361', new_item)


def indirect_book_identifier(wd, id_value):
    """
    Like `indirect_identifier` above but to accomodate ISBN-specific weirdness.

    @param wd: a WikidataEntry object
    @param id_value: the identifier of the other work
    """

    if id_value in isbn_map:
        relevant_item = isbn_map[id_value]
        wd.append('itemid', 'P361', relevant_item)
    else:
        # TODO: Actually build this in to BiblioWikidata
        citoid = requests.get(
            'https://en.wikipedia.org/api/rest_v1/data/citation/mediawiki/' +
            id_value).json()
        if len(citoid) == 1:
            if 'title' in citoid[0]:
                book_title = citoid[0]['title']
            else:
                book_title = 'Untitled'
        else:
            book_title = 'Untitled'

        if len(id_value) == 10:
            prop_nr = 'P957'
        else:
            prop_nr = 'P212'

        book_item = wdi_core.WDItemEngine(
            item_name=book_title,
            domain='books',
            data=[
                wdi_core.WDItemID(value='Q3331189', prop_nr='P31'),
                wdi_core.WDExternalID(id_value, prop_nr=prop_nr)
            ])

        if book_title != 'Untitled':
            book_item.set_label(book_title)

        try:
            res = book_item.write(WIKI_SESSION)
            print(res)
            isbn_map[id_value] = res
            wd.append('itemid', 'P361', res)
        except Exception as e:
            print('Creating item for ISBN ' + id_value + ' failed: ' + e)


def get_class(class_string):
    """
    Converts a string document type into an appropriate Wikidata item.

    This is technically a generator because there are certain circumstances
    where I will want to yield *two* values, so this accounts for that.

    @param class_string: string representing document type from NIOSHTIC
    """

    class_string = class_string.lower().strip()  # just in case

    if class_string == 'book or book chapter':
        # We are going to put down that it's both and we'll sort it later
        yield 'Q3331189'
        yield 'Q1980247'

    # Reports
    elif class_string in [
            'report of investigation', 'report of investigations',
            'final contract report', 'final grant report',
            'final cooperative agreement report', 'control technology',
            'purchase order report', 'current intelligence bulletin',
            'task order report', 'hazard id', 'impact sheet'
    ]:
        yield 'Q10870555'

    # Field studies
    elif class_string in [
            'fatality assessment and control evaluation',
            'hazard evaluation and technical assistance',
            'hazard evaluation and technical assistan',  # yes
            'technical assistance',
            'health hazard evaluation',
            'field studies',
            'field study',
            'field studies\' industry wide',
            'industry wide'
    ]:
        yield 'Q26840222'

    elif class_string in [
            'conference/symposia proceedings',
            'conference/symposia proceedingsr',
            'conference/symposia proceeedings',
            'conference/symposia proceeding'
    ]:
        yield 'Q23927052'

    elif class_string in ['abstract', 'abstracts']:
        yield 'Q333291'

    elif class_string in ['research dataset', 'surveillance dataset']:
        yield 'Q1172284'

    elif class_string == 'journal article':
        yield 'Q13442814'

    elif class_string == 'book':
        yield 'Q3331189'

    elif class_string == 'chapter':
        yield 'Q1980247'

    elif class_string in ['patent', 'pa']:
        yield 'Q253623'

    elif class_string == 'testimony':
        yield 'Q1196258'

    elif class_string == 'criteria document':
        yield 'Q26840225'

    elif class_string == 'dissertation':
        yield 'Q187685'

    elif class_string == 'formal presentation':
        yield 'Q603773'

    elif class_string == 'video':
        yield 'Q34508'

    elif class_string == 'thesis':
        yield 'Q1266946'

    elif class_string == 'technical report':
        yield 'Q3099732'

    elif class_string == 'poster':
        yield 'Q429785'

    elif class_string == 'mobile app':
        yield 'Q620615'


def get_priority_area(input_string):
    """
    Converts a string priority area into an appropriate Wikidata item.

    @param input_string: string representing priority area from NIOSHTIC
    """

    mapping = {
        'AIDS-virus':
        'Q15787',
        'Allergic and Irritant Dermatitis':
        'Q18556308',
        'Asthma and Chronic Obstructive Pulmonary Disease':
        'Q2551913',
        'Cancer':
        'Q2936210',
        'Cancer Research Methods':
        'Q3421914',
        'Cardiovascular Disease':
        'Q23900716',
        'Cardiovascular-disease':
        'Q23900716',
        'Cardiovascular-diseases':
        'Q23900716',
        'Construcci&#243':
        'Q385378',
        'Construction':
        'Q385378',
        'Control Technology &amp':
        'Q24884545',
        'Control Technology and Personal Protective Equipment':
        'Q1333024',
        'Control-technology':
        'Q24884545',
        'Dermatitis':
        'Q18556308',
        'Disease and Injury':
        'Q637816',
        'Disease and Injury: Allergic and Irritant Dermatitis':
        'Q18556308',
        'Disease and Injury: Asthma and Chronic Obstructive Pulmonary Disease':
        'Q2551913',
        'Disease and Injury: Fertility and Pregnancy Abnormalities':
        'Q32984820',
        'Disease and Injury: Hearing Loss':
        'Q20887524',
        'Disease and Injury: Infectious Diseases':
        'Q26989024',
        'Disease and Injury: Low Back Disorders':
        'Q26989028',
        'Disease and Injury: Musculoskeletal Disorders of the Upper Extremities':
        'Q26989030',
        'Disease and Injury: Traumatic Injuries':
        'Q26882978',
        'Emerging Technologies':
        'Q120208',
        'Exposure Assessment Methods':
        'Q4008388',
        'Fertility and Pregnancy Abnormalities':
        'Q32984820',
        'Health Services Research':
        'Q2518253',
        'Hearing Loss':
        'Q20887524',
        'Indoor Environment':
        'Q26989033',
        'Infectious Diseases':
        'Q26989024',
        'Intervention Effectiveness Research':
        'Q26989040',
        'Investigation of Adverse Effects':
        'Q2047938',
        'Low Back Disorders':
        'Q26989028',
        'Manufacturing':
        'Q187939',
        'Migrant-workers':
        'Q15320003',
        'Mining':
        'Q44497',
        'Mining: Oil and Gas Extraction':
        'Q26989041',
        'Musculoskeletal -System - disorders':
        'Q4116663',
        'Musculoskeletal Disorders of the Upper Extremities':
        'Q26989030',
        'Musculoskeletal System Disorders':
        'Q4116663',
        'Musculoskeletal- system-disorders':
        'Q4116663',
        'Musculoskeletal-system':
        'Q726543',
        'Musculoskeletal-system- disorders':
        'Q4116663',
        'Musculoskeletal-system-disorders':
        'Q4116663',
        'Neurotoxic Disorders':
        'Q26989045',
        'Neurotoxic Effects':
        'Q3338704',
        'Neurotoxic- effects':
        'Q3338704',
        'Neurotoxic-effect':
        'Q3338704',
        'Neurotoxic-effects':
        'Q3338704',
        'Noise-induced-hearing-loss':
        'Q1475712',
        'Occupational Health Disparities':
        'Q26989052',
        'Oil and Gas':
        'Q26989041',
        'Oil and Gas Extraction':
        'Q26989041',
        'Organization of Work':
        'Q26989053',
        'Personal Protective Equipment':
        'Q1333024',
        'Personal Protective Technology':
        'Q1333024',
        'Psychologic Disorders':
        'Q12135',
        'Psychological-disorders':
        'Q12135',
        'Public Safety':
        'Q294240',
        'Pulmonary System Disorders':
        'Q7075805',
        'Pulmonary-system':
        'Q7891',
        'Pulmonary-system- disorders':
        'Q7075805',
        'Pulmonary-system-disorders':
        'Q7075805',
        'Reproductive System Disorders':
        'Q32984820',
        'Reproductive- system-disorders':
        'Q32984820',
        'Reproductive-system-disorder':
        'Q32984820',
        'Reproductive-system-disorders':
        'Q32984820',
        'Research Tools and Approaches: Cancer Research Methods':
        'Q3421914',
        'Research Tools and Approaches: Control Technology &amp':
        'Q24884545',
        'Research Tools and Approaches: Control Technology and Personal Protective Equipment':
        'Q24884545',
        'Research Tools and Approaches: Exposure Assessment Methods':
        'Q4008388',
        'Research Tools and Approaches: Health Services Research':
        'Q2518253',
        'Research Tools and Approaches: Intervention Effectiveness Research':
        'Q26989040',
        'Research Tools and Approaches: Risk Assessment Methods':
        'Q1058438',
        'Research Tools and Approaches: Surveillance Research Methods':
        'Q32984836',
        'Respirator Research':
        'Q32984837',
        'Respirators':
        'Q271779',
        'Respiratory- system-disorders':
        'Q7075805',
        'Respiratory-system':
        'Q7891',
        'Respiratory-system-disorders':
        'Q7075805',
        'Risk Assessment Methods':
        'Q1058438',
        'Services: Public Safety':
        'Q294240',
        'Surveillance Research Methods':
        'Q32984836',
        'Training':
        'Q32984842',
        'Traumatic Injuries':
        'Q26882978',
        'Work Environment And Workforce: Emerging Technologies':
        'Q120208',
        'Work Environment And Workforce: Indoor Environment':
        'Q26989033',
        'Work Environment And Workforce: Organization of Work':
        'Q26989053',
        'Work Environment and Workforce: Emerging Technologies':
        'Q120208',
        'Work Environment and Workforce: Indoor Environment':
        'Q26989033',
        'Work Environment and Workforce: Organization of Work':
        'Q26989053',
        'Mixed Exposures':
        'Q32984844',
        'Work Environment And Workforce: Mixed Exposures':
        'Q32984844',
        'Work Environment and Workforce: Mixed Exposures':
        'Q32984844'
    }

    if input_string in mapping:
        return mapping[input_string]

    return None


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
        self.label = None  # i.e. no custom set label, yet
        self.description = None  # see above

        self.ref = [[
            wdi_core.WDItemID(
                value='Q26822184', prop_nr='P248', is_reference=True),
            wdi_core.WDExternalID(
                nioshtic_blob['NN'], prop_nr='P2880', is_reference=True),
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

    def get_referenced_statements(self, prop_nr):
        """
        Get the *referenced* statements given a `prop_nr`.

        Why referenced? We don't really care about statements that do not have
        references, since our edit will add the reference. However, a statement
        with a reference already does not need any additional work.

        @param prop_nr: string of Wikidata property number (e.g. P31)
        @return list of referenced statement values given the property number
        """

        claims = self.raw['claims']

        if prop_nr not in claims:
            return []

        res = []
        for claim in claims[prop_nr]:
            if 'references' in claim:
                if len(claim['references']) > 0:  # just in case
                    res.append(claim)

        return res

    def append(self, datatype, prop_nr, value, qualifiers=[]):
        """
        Append a statement to a WikidataEntry.

        @param datatype: string, externalid, itemid, or date
        @param prop_nr: string Wikidata property ID (e.g. P31)
        @param value: string representing the value of the statement
        @param qualifiers: list of qualifiers that are WDBaseDataType children
        """
        statement = ''

        if len(value) == 0:
            return

        if datatype == 'string':
            statement = wdi_core.WDString(
                value=value,
                prop_nr=prop_nr,
                references=self.ref,
                qualifiers=qualifiers)
        elif datatype == 'monolingual':
            statement = wdi_core.WDMonolingualText(
                value=value,
                prop_nr=prop_nr,
                references=self.ref,
                qualifiers=qualifiers,
                language='en')
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
            raise ValueError(
                '`datatype` should be string, externalid, itemid, or date')

        self.data.append(statement)

    def set_label(self, t):
        """
        Sets the label of the item described by the item engine.

        @param t: string
        """

        self.label = t

    def set_description(self, t):
        """
        Sets the description of the item described by the item engine.

        @param t: string
        """

        self.description = t

    def save(self):
        # Creating item engine anew since there is no clear way to assign data
        # to an existing one.

        if len(self.data) > 0:
            eq.post(self.wikidata_id, self.data, self.label, self.description)


def fill(nioshtic_data):
    """
    Fill out several Wikidata items based on NIOSHTIC data

    @param nioshtic_data: a dictionary with lots of NIOSHTIC data
    """

    for entry in nioshtic_data['entries']:
        if 'Wikidata' in entry:
            wd = WikidataEntry(entry, nioshtic_data['retrieved'])
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
            "PA" = priority area, semicolon-delineated (use as main subject)
            "IB" = ISBN (10 or 13) for either a chapter or a book
            "Author Keywords"/"Author keywords" = see KW
            '''

            is_main_work = False
            if 'DT' in entry:
                if 'book' in entry['DT'] or 'journal article' in entry['DT']:
                    is_main_work = True

            # Check if there are identifiers we can add
            if wd.has_property('P356') is False \
            or wd.has_property('P698') is False \
            or wd.has_property('P932') is False:

                if 'LT' in entry:
                    identifiers = URLtoIdentifier.convert(entry['LT'])

                    if identifiers['doi'] is not None \
                    and wd.has_property('P356') is False:
                        if is_main_work is True:
                            wd.append('externalid', 'P356',
                                      JournalArticles.clean_title(
                                          identifiers['doi']))
                        else:
                            indirect_identifier(wd, 'doi',
                                                JournalArticles.clean_title(
                                                    identifiers['doi']))

                    if identifiers['pmid'] is not None \
                    and wd.has_property('P698') is False:
                        if is_main_work is True:
                            wd.append('externalid', 'P698',
                                      JournalArticles.clean_title(
                                          identifiers['pmid']))
                        else:
                            indirect_identifier(wd, 'pmid',
                                                JournalArticles.clean_title(
                                                    (identifiers['pmid'])))

                    if identifiers['pmcid'] is not None\
                    and wd.has_property('P932') is False:
                        if is_main_work is True:
                            wd.append('externalid', 'P932',
                                      JournalArticles.clean_title(
                                          identifiers['pmcid']))
                        else:
                            indirect_identifier(wd, 'pmcid',
                                                JournalArticles.clean_title(
                                                    identifiers['pmcid']))

            # instance of
            extant_instanceof = wd.get_referenced_statements('P31')
            extant_instanceof = [x['mainsnak']['datavalue']['value']['id'] \
                                 for x in extant_instanceof]
            if 'DT' in entry:
                # First we look for a *specific type* of publication.
                # Failing that, we fall back to a generic "publication" tag.
                found = False
                for thing in entry['DT']:
                    for instanceof in get_class(
                            JournalArticles.clean_title(thing)):
                        found = True
                        wd.append('itemid', 'P31', instanceof)

                if found is False:
                    if 'Q732577' not in extant_instanceof:
                        wd.append('itemid', 'P31', 'Q732577')
            else:
                if 'Q732577' not in extant_instanceof:
                    wd.append('itemid', 'P31', 'Q732577')

            # publication date
            if wd.has_property('P577') is False and 'DP' in entry:
                year = entry['DP'][:4]
                month = entry['DP'][4:6]
                day = entry['DP'][6:8]

                date = '+' + year + '-' + month + '-' + day + 'T00:00:00Z'

                wd.append('date', 'P577', date)

            # title
            if 'TI' in entry:
                t = JournalArticles.clean_title(entry['TI'])
                extant_titles = wd.get_referenced_statements('P1476')
                extant_titles = [x['mainsnak']['datavalue']['value']['text'] \
                                 for x in extant_titles \
                                 if len(x['references'][0]['snaks']) >= 3]
                if t not in extant_titles:
                    if len(t) <= 400:
                        wd.append('monolingual', 'P1476', t)
                    if len(t) <= 250:
                        wd.set_label(t)

            # authors
            if wd.has_property('P2093') is False \
            and wd.has_property('P50') is False \
            and 'AU' in entry:
                authors = entry['AU'].split(';')

                for ordinal, author in enumerate(authors):
                    wd.append(
                        'string',
                        'P2093',
                        JournalArticles.clean_title(author),
                        qualifiers=[
                            wdi_core.WDString(
                                value=str(ordinal + 1),
                                prop_nr='P1545',
                                is_qualifier=True)
                        ])

            # ISBN
            if wd.has_property('P957') is False \
            and wd.has_property('P212') is False:
                if 'IB' in entry:
                    cleaned_isbn = entry['IB'].replace('-', '')
                    cleaned_isbn = JournalArticles.clean_title(cleaned_isbn)
                    if is_main_work is True:
                        if len(cleaned_isbn) == 10:
                            isbn_prop = 'P957'
                        elif len(cleaned_isbn) == 13:
                            isbn_prop = 'P212'
                        wd.append('externalid', isbn_prop, cleaned_isbn)
                    else:
                        indirect_book_identifier(wd, cleaned_isbn)

            # volume, issue, pages
            if wd.has_property('P478') is False \
            or wd.has_property('P433') is False \
            or wd.has_property('P304') is False:
                if 'SO' in entry:
                    strict_test = re.match(r'.* (\d+)\((.*)\):(.+)',
                                           entry['SO'])
                    if strict_test:
                        volume = strict_test.group(1)
                        issue = strict_test.group(2)
                        pages = strict_test.group(3)

                        wd.append('string', 'P478', volume)
                        wd.append('string', 'P433', issue)
                        wd.append('string', 'P304', pages)

            # time to process main subjects

            # CAS numbers
            if 'CN' in entry:
                cas_numbers = entry['CN'].split(';')
                cas_numbers = [
                    JournalArticles.clean_title(x) for x in cas_numbers
                ]
                for cas_number in cas_numbers:
                    if cas_number in cas_map:
                        wd.append('itemid', 'P921', cas_map[cas_number])

            if 'PA' in entry:
                priority_areas = entry['PA'].split(';')
                priority_areas = [
                    JournalArticles.clean_title(x) for x in priority_areas
                ]
                for area in priority_areas:
                    area_item = get_priority_area(area)
                    if area_item is not None:
                        wd.append('itemid', 'P921', area_item)

            # After everything is done:

            wd.save()


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

    eq.done()


if __name__ == '__main__':
    main()
