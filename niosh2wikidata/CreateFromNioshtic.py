#!/usr/local/bin/python3.6

# Notes:
# If the SO ends with ' :1' then skip over it.
# Be able to find a way to translate DT into "instance of". Some documents have
# multiple DTs separated by semicolons or whatever
# Creating items on books; Associating chapters with books. Really hard because
# of lousy metadata!

# General workflow: create barebones items for NIOSHTIC numbers that do not have
# entries. Then, once anything that deserves an entry gets an entry,
# the additional metadata can be added
#
# Entries initially have: label, NIOSHTIC number, sponsored by: NIOSH,
# title property

import json
import os
import re
from wikidataintegrator import wdi_core, wdi_login
from wikidata_credentials import *

try:
    from libs.BiblioWikidata import JournalArticles
except ImportError:
    raise ImportError('Did you remember to `git submodule init` '
                      'and `git submodule update`?')

WIKI_SESSION = wdi_login.WDLogin(user=wikidata_username, pwd=wikidata_password)


def process_data(nioshtic_data):
    """
    Creates Wikidata items on most NIOSHTIC entries.

    Before you execute this method, make sure you have executed CreateJournalArticles.py
    and then Associator.py.

    This only handles creation. Filling in the columns from the rest of the
    NIOSHTIC dataset is handled by a separate class.

    @param nioshtic_data: dictionary with "entries" and "headers" keys
    """

    for entry in nioshtic_data['entries']:
        if 'Wikidata' in entry or 'NN' not in entry:
            continue

        if 'TI' not in entry:
            continue

        if 'SO' in entry:
            if entry['SO'].endswith(' :1'):
                continue  # Only one page, most likely a flyer

        if re.match(r'Youth@Work', entry['TI']) is not None \
        and re.match(r'edition', entry['TI']) is not None:
            continue

        ref = [[
            wdi_core.WDItemID(
                value='Q26822184', prop_nr='P248', is_reference=True),
            wdi_core.WDExternalID(
                entry['NN'], prop_nr='P2880', is_reference=True),
            wdi_core.WDTime(
                nioshtic_data['retrieved'], prop_nr='P813', is_reference=True)
        ]]

        data = [
            wdi_core.WDExternalID(
                entry['NN'], prop_nr='P2880', references=ref),
            wdi_core.WDItemID(value='Q60346', prop_nr='P859'),
            wdi_core.WDMonolingualText(
                value=entry['TI'],
                prop_nr='P1476',
                references=ref,
                language='en')
        ]

        t = JournalArticles.clean_title(entry['TI'])
        i = wdi_core.WDItemEngine(
            data=data, domain='nioshgreylit', item_name=t)
        i.set_label(t)

        try:
            print(i.write(WIKI_SESSION))
        except Exception as e:
            print(e)
            continue


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
