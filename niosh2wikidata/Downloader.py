#!/usr/local/bin/python3

"""
Downloads records from NIOSHTIC and prepares text files.
"""

import requests

def get_content(start_date, end_date, search_term=''):
	"""
	Handles the primary work of downloading from NIOSHTIC and getting the raw
	content.

	@param start_date: month/year string in the style '01-2017'
	@param end_date: month/year string in the style '12-2017'
	@return gigantic string containing the NIOSHTIC output
	"""

	session = requests.Session()

	# Initialize session cookies
	session.get('https://www2a.cdc.gov/nioshtic-2/advsearch2.asp')

	# Execute search
	session.get('https://www2a.cdc.gov/nioshtic-2/BuildQyr.asp'
		        '?s1={0}'
		        '&f1=TI'
		        '&t1=0'
		        '&s2='
		        '&f2=TI'
		        '&t2=0'
		        '&s3='
		        '&f3=TI'
		        '&terms=3'
		        '&Adv=1'
		        '&n=new'
		        '&View=b'
		        '&Startyear={1}'
		        '&EndYear={2}'
		        '&whichdate=DP'
		        '&D1=10'
		        '&Limit=25000'
		        '&Sort=DP+DESC'
		        '&ct='
		        '&B1=Search'.format(search_term, start_date, end_date))

	# Trigger download
	session.get('https://www2a.cdc.gov/nioshtic-2/Download.asp'
		        '?s1={0}'
		        '&f1=TI'
		        '&Startyear={1}'
		        '&terms=3'
		        '&Adv=1'
		        '&ct='
		        '&Limit=25000'
		        '&Sort=DP+DESC'
		        '&whichdate=DP'
		        '&D1=10'
		        '&EndYear={2}'
		        '&View=b'
		        '&PageNo=1'
		        '&RecordSet=0'.format(search_term, start_date, end_date))

	# Carry out download
	t = session.get('https://www2a.cdc.gov/nioshtic-2/TICDownload.ASP'
		            '?submit1=Download'
		            '&RS1=0'
		            '&DownloadCount=25000'
		            '&DownloadStart=1'
		            '&select1=f'
		            '&recordset=0')

	return t.text

def create_text_file(start_date, end_date, search_term=''):
	"""
	Creates and saves a text file of the get_content output.

	See the docstring for the get_content method for API information.
	"""

	to_write = get_content(start_date, end_date, search_term)

	parts = [start_date.split('-'), end_date.split('-')]

	filename = 'raw/' + \
	           parts[0][1] + '-' + parts[0][0] + '_to_' + \
	           parts[1][1] + '-' + parts[1][0] + '.txt'

	with open(filename, 'w') as f:
		f.write(to_write)

def main():
	"""
	Create text files for January 1900 to December 2024.
	"""

	date_ranges = [
		('01-1900', '12-1959'),
		('01-1960', '12-1964'),
		('01-1965', '12-1969'),
		('01-1970', '12-1974'),
		('01-1975', '12-1979'),
		('01-1980', '12-1984'),
		('01-1985', '12-1989'),
		('01-1990', '12-1994'),
		('01-1995', '12-1999'),
		('01-2000', '12-2004'),
		('01-2005', '12-2009'),
		('01-2010', '12-2014'),
		('01-2015', '12-2019'),
		('01-2020', '12-2024')
	]

	for date_range in date_ranges:
		print('Downloading: ' + date_range[0] + ' to ' + date_range[1])
		create_text_file(date_range[0], date_range[1])

if __name__ == '__main__':
	main()