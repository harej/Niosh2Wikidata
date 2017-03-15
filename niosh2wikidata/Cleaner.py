#!/usr/local/bin/python3

"""
Takes raw output from NIOSHTIC and converts it into a format usable by the rest
of the Niosh2Wikidata library.
"""

import re
import json
import os

def fix_format(to_process):
    """
    Fixes weird formatting in the NIOSHTIC output.

    @param to_process: string representing the file being cleaned
    @return more consistently formatted string
    """

    DIVIDER = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

    cleaned_string = to_process

    # Fix weird newline and space issues
    cleaned_string = re.sub(r'\n {4}', ' ', cleaned_string)
    cleaned_string = re.sub(r'\n{2,}', '\n', cleaned_string)
    cleaned_string = re.sub(r'\n^(?!.+: )', ' ', cleaned_string)
    cleaned_string = re.sub(r' {2,}', ' ', cleaned_string)

    # Get rid of header stuff
    cleaned_string = cleaned_string.split(DIVIDER)[2]

    return cleaned_string

def clean(to_process):
    """
    Takes raw output from NIOSHTIC and produces a cleaned up dictionary.

    @param to_process: the string representing the file being cleaned
    @return dict of cleaned up content
    """

    cleaned_string = fix_format(to_process)

    headers = []  # list of strings
    entries = []  # list of dictionaries
    entry_counter = -1

    lines = cleaned_string.split('\n')

    for line in lines:
        line = line.strip(' \t\n\r')
        if line == '':
            continue
        pair = line.split(':', 1)  # "NN: 123456"
        if len(pair) != 2:
            continue
        rowkey = pair[0]
        rowvalue = pair[1]
        if rowvalue == '':
            continue
        elif rowvalue[0] == ' ':
            rowvalue = rowvalue[1:]  # truncating leading space
        if rowkey == 'NN':  # NN delineates new entries
            entry_counter += 1
            entries.append({})

        if rowkey not in headers:
            headers.append(rowkey)

        entries[entry_counter][rowkey] = rowvalue

    return {'headers': headers, 'entries': entries}


def process_file(filename):
    """
    Loads a file, runs it through the clean method, and saves a new file.

    @param filename: name of file to process (e.g. output.txt)
    @return new file (e.g. output.txt.json)
    """

    new_content = {}
    with open(filename, encoding='ISO-8859-1') as f:
        blob = ''
        for line in f:
            blob += line
        blob = blob.replace('\r\n', '\n')  # just in case
        new_content = clean(blob)

        new_filename = filename + '.json'

        with open(new_filename, 'w') as nf:
            json.dump(new_content, nf)

        print("Processed file save to: " + new_filename)


def main():
    """
    If this file is invoked from command line, autodiscover textfiles in the
    raw/ subdirectory and process them.
    """

    for filename in os.listdir('raw/'):
        if filename.lower().endswith('.txt'):
            process_file('raw/' + filename)

if __name__ == '__main__':
    main()
