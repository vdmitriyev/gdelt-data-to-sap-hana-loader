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
		self.data_csvdata_files = fi.iterate_through_catalog(DATA_DIRECTORY)	

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

	def fetch_row_into_str(self, row):
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


	def escapeinput_data_for_sql(self, value, sql_type):
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

	def build_query_part(self, input_data, table_fields_types, query_part):
		"""
			(obj, list, list, list, boolean) -> (str)

			Building part of the query, according to the value passed with 'query_part' parameter (should be 1 or 2).
		"""

		result_query = '('

		for index in xrange(len(input_data)):

			if query_part == 1:
				proper_value = '"' + input_data[index] + '"'
				
			if query_part == 2:
				proper_value = self.escapeinput_data_for_sql(input_data[index], table_fields_types[index])
				
			result_query = result_query + proper_value + ','

		result_query = result_query[:len(result_query)-1]
		result_query = result_query + ')'

		return result_query



	def form_insert_query(self, table_name, input_data, table_fields_names=None, table_fields_types=None):
		"""
			(obj, str, list, list) -> (str)

			Returning "insert" SQL statement with values.
		"""

		# creating first part of the query -> section with columns' names
		query_table_structure = self.build_query_part(table_fields_names, table_fields_types, query_part=1)

		# creating second part of the query -> section with values
		query_values = self.build_query_part(input_data, table_fields_types, query_part=2)
		
		# form query
 		query = 'INSERT INTO ' + table_name + ' ' + query_table_structure + ' VALUES ' + query_values

		return query

	def identify_table_mask(self, maskdata_file_name='daily_update_table-mask.txt', delim=';'):
		"""
			(obj, str) -> (list(), list())

			Extracting table identifiers from the ".txt" mask file.
			'Table Definitions' are taken by simple "Copy->Paste" from 'Open Definition' visual interface of table in SAP HANA Studio.
		"""

		table_fields_names, table_fields_types = list(), list()		 

		mask_f = open(META_INFO_DIRECTORY + '/' + maskdata_file_name, "r")

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

		return table_fields_names, table_fields_types

	def check_if_row_already_loaded(self, row, file_name):
		"""
			(obj,) -> boolean

			Checking if data is already loaded into db's table.
		"""
		query = "SELECT count(*) FROM " + TABLE_NAME + " WHERE GLOBALEVENTID = " + "'" + row[0] + "'"

		try:			
			# print query
			cursor = self.connection.cursor()
			executed_cur = cursor.execute(query)

			if executed_cur:			
				result_cur = cursor.fetchall()
				for row in result_cur:
					if int(row[0]) > 0:
						return True
			else:
				print "[e] Something wrong with execution."
		except Exception, e:
			print '[e] Exeption: %s while processing "%s" file in method %s' % \
                  (str(e), DATA_DIRECTORY + '/' + file_name, "check_if_row_already_loaded")
			print '\t[q] Query that caused exception \n %s' % (query)


		return False

	def is_valid_row_to_insert(self, row):
		"""
			(obj, list) -> boolean

			Checking if row is to be valid to inserrted.
		"""

		if row[5] == COUNTRY or row[15] == COUNTRY:
			return True
		return False

	def insert_data(self, row, table_fields_names, table_fields_types, data_file):
		"""
			(obj, list, list, list) -> NoneType

			Inserting one single row to table.
		"""

		try:				
			query = self.form_insert_query(TABLE_NAME, row, table_fields_names, table_fields_types)
			self.execute_query(query)							
		except Exception, e:				
			print '[e] Exeption: %s  while processing "%s" file' % (str(e), DATA_DIRECTORY + '/' + data_file)
			print '\t[q] Query that caused exception \n %s' % (query)
			return False

		return True

	def load_data_from_file(self, data_file, table_fields_names, table_fields_types, check_and_skip=False):
		"""
			(obj, str, list, list, boolean) -> (int, int)

			Loading data from specified file.
		"""

		print '[i] Processing file: %s' % (data_file)

		success_queries, error_queries = 0, 0

		csv_f = open(DATA_DIRECTORY + '/' + data_file, "r")
		line = csv_f.readline()
		_continue_processing = True

		if check_and_skip:
			while line:
				row = self.line_to_list(line)
				if self.is_valid_row_to_insert(row):
					if self.check_if_row_already_loaded(row, data_file):
						print '[i]  Skipped file: %s' % (data_file)
						_continue_processing = False					
					break
				line = csv_f.readline()

		if _continue_processing:
			while line:				
				row = self.line_to_list(line)
				if self.is_valid_row_to_insert(row):
					if self.insert_data(row, table_fields_names, table_fields_types, data_file):
						success_queries = success_queries + 1
					else:
						error_queries = error_queries + 1
				line = csv_f.readline()

		csv_f.close()

		if not check_and_skip or success_queries > 0:
			print '[i] Total queries processed from file "%s": %d' % (data_file, success_queries)

		return success_queries, error_queries

	def load_gdeltinput_data_to_db(self, truncate_table=False, skip_loaded_files=False):
		"""
			(obj) -> NoneType

			Fetching data from CSV with GDELT data and loading to database (with insert statements).
		"""

		table_fields_names, table_fields_types = self.identify_table_mask()

		# Truncating table
		if truncate_table:
			query = 'TRUNCATE TABLE '  + TABLE_NAME;
			try:
				self.execute_query(query)
			except Exception, e:
				print '[e] Exeption: %s' % (str(e))

		total_queries = 0
		total_error_queries = 0

		for directory in self.data_csvdata_files:
			for data_file in self.data_csvdata_files[directory]:				
				success_queries, error_queries = self.load_data_from_file(data_file, table_fields_names, table_fields_types, skip_loaded_files)
				total_queries = total_queries + success_queries
				total_error_queries = total_error_queries + error_queries

		print '\n[i] Queries processed in total: %d\n' % (total_queries)

		if total_error_queries > 0:
			print '[i] Queries processed in total with errors: %d' % (total_error_queries)
		
def main():
	"""
		(NoneType) -> NoneType

		Main method that creates objects and start processing.
	"""

	gdl = GDELTDataLoader()
	# gdl.load_gdeltinput_data_to_db(truncate_table=False)
	gdl.load_gdeltinput_data_to_db(truncate_table=False, skip_loaded_files=True)


if __name__ == '__main__':
	main()
