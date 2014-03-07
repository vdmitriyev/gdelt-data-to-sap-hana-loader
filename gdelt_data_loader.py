# coding: utf-8
import dbapi
import traceback
import sap_hana_credentials as credentials
from folder_iterator import FolderIterator

DATA_DIRECTORY = 'data'
META_INFO_DIRECTORY = 'meta-info'
TABLE_NAME = '"DEMOUSER00"."uni.vlba.gdelt.data::gdelt_dailyupdates"'
COUNTRY = 'KAZ'

"""
    Author  : Viktor Dmitriyev
    Goal 	: Establish connection to SAP HANA DB using shipped with SAP HANA python client routine(dpapi) 
    		  and loading GDELT database from daily updates.
    Date    : 06.03.2014
"""

class GDELTDataLoader():

	def __init__(self):
		"""
			Init method that creates connection and iterates data folder.
		"""

		self.connection = self.get_connection()
		fi = FolderIterator()
		self.data_csv_files = fi.iterate_through_catalog(DATA_DIRECTORY)	

	def get_connection(self):
		"""
			(obj) -> (obj)

			Method that will return connection to the database using given credentials.
		"""

		return  dbapi.connect(credentials.SERVER,\
							  credentials.PORT,\
							  credentials.USER,\
							  credentials.PASSWORD)

	def _build_test_query01(self):
		"""
			(obj) -> (str)

			Building query for execution
		"""

		query =  'select "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year" ' + \
					 'from "DEMOUSER00"."uni.vlba.gdelt.data::gdelt_dailyupdates"'		

		return query

	def _fetch_row_into_str(self, row):
		"""
			(list) -> (str)

			Fetching values from the given row(tuple) that are presented in form of list.
		"""

		str_row = ""
		for value in row:		
			str_row = str_row + str(value) + ' | \t\t'
		return str_row[:-5]


	def execute_query(self, query, fetch=False):		
		"""
			(obj, str) -> NoneType

			Running given query and using given connection. 
			Fetching result rows and printing them to standard output.
		"""

		cursor = self.connection.cursor()
		executed_cur = cursor.execute(query)

		if executed_cur:
			if fetch:
				result_cur = cursor.fetchall()
				for row in result_cur:
						print fetch_row_into_str(row)
		else:
			print "[e] Something wrong with execution."

	def line_to_list(self, _line):
		"""
			(obj, str) -> list()

			Converting input line that suppose to be an csv to the separated list.
		"""

		result = list()		
		_line_splited = _line.split('\t')

		for value in _line_splited:
			value_stripped = value.strip().rstrip()			
			result.append(value_stripped)				
		
		return result


	def escape_data_for_sql(self, value, sql_type):
		"""
			(obj, str) -> str

			Escape symbols to be used in sql statements.
		"""
		value = value.replace('\'','"')
		
		if len(value) == 0:
			if sql_type in ('BIGINT', 'INTEGER', 'FLOAT', 'DOUBLE'):
				return '0'
			if sql_type == 'NVARCHAR':
				return '\'\''
		else:
			if sql_type in ('BIGINT', 'INTEGER', 'FLOAT', 'DOUBLE'):
				# return value
				return '\'' + value + '\''
			if sql_type == 'NVARCHAR':
				return '\'' + value + '\''

		return ''

	def build_query_part(self, _data, table_fields_types, query_part):
		"""
			(obj, list, list, list, boolean) -> (str)

			Building part of the query.
		"""

		result_query = '('

		for index in xrange(len(_data)):

			if query_part == 1:
				proper_value = '"' + _data[index] + '"'
				
			if query_part == 2:
				proper_value = self.escape_data_for_sql(_data[index], table_fields_types[index])
				

			result_query = result_query + proper_value + ','

		result_query = result_query[:len(result_query)-1]
		result_query = result_query + ')'

		return result_query



	def form_insert_query(self, table_name, _data, table_fields_names=None, table_fields_types=None):
		"""
			(obj, str, list, list) -> (str)

			Returning "insert" SQL statement with values.
		"""	

		# creating first part of the query -> section with columns' names
		query_table_structure = self.build_query_part(table_fields_names, table_fields_types, query_part=1)

		# creating second part of the query -> section with values
		query_values = self.build_query_part(_data, table_fields_types, query_part=2)
		
		# form query
 		query = 'INSERT INTO ' + table_name + ' ' + query_table_structure + ' VALUES ' + query_values

		return query

	def identify_table_mask(self, mask_file_name='daily_update_table-mask.txt', delim=';'):
		"""
			(obj, str) -> (list(), list())

			Extracting table identifiers from the ".txt" mask file.
			'Table Definitions' are taken by simple "Copy->Paste" from 'Open Definition' visual interface of table in SAP HANA Studio.
		"""

		table_fields_names, table_fields_types = list(), list()		 

		mask_f = open(META_INFO_DIRECTORY + '/' + mask_file_name, "r")

		# skipping line with descriptions of attributes
		line = mask_f.readline()

		# first line with data
		line = mask_f.readline()
		while line:
			value_list = line.split(delim)
			table_fields_names.append(value_list[0])
			table_fields_types.append(value_list[1])
			line = mask_f.readline()
			
		mask_f.close()

		return table_fields_types, table_fields_names


	def load_gdelt_data_to_db(self, truncate_table=False):
		"""
			(obj) -> NoneType

			Fetching data from CSV with GDELT data and loading to database (with insert statements).
		"""

		table_fields_types, table_fields_names = self.identify_table_mask()
		
		# Truncating Table
		if truncate_table:
			query = 'TRUNCATE TABLE '  + TABLE_NAME;
			try:
				self.execute_query(query)
			except Exception, e:
				print '[e] Exeption: %s' % (str(e))

		total_queries = 0
		total_error_queries = 0

		for directory in self.data_csv_files:
			for _file in self.data_csv_files[directory]:
				sub_queries = 0
				csv_f = open(DATA_DIRECTORY + '/' + _file, "r")
				print '[i] Processing file: %s' % (_file)
				line = csv_f.readline()
				while line:
					values_list = self.line_to_list(line)
					if values_list[5] == COUNTRY or values_list[15] == COUNTRY:
						query = self.form_insert_query(TABLE_NAME, values_list, table_fields_names, table_fields_types)
						try:
							sub_queries = sub_queries + 1
							self.execute_query(query)							
						except Exception, e:
							total_error_queries = total_error_queries + 1
							print '[e] Exeption: %s  while processing "%s" file' % (str(e), DATA_DIRECTORY + '/' + _file)
							print '\t[q] Query that caused exception \n %s' % (query)

					line = csv_f.readline()				
				csv_f.close()
				print '[i] Total queries processed from file "%s": %d' % (_file, sub_queries)
				total_queries = total_queries + sub_queries				

		print '\n[i] Queries processed in total: %d\n' % (total_queries)

		if total_error_queries > 0:
			print '[i] Queries processed in total with errors: %d' % (total_error_queries)
		
def main():
	"""
		(NoneType) -> NoneType

		Main method that creates objects and start processing.
	"""

	gdl = GDELTDataLoader()
	gdl.load_gdelt_data_to_db(truncate_table=True)


if __name__ == '__main__':
	main()
