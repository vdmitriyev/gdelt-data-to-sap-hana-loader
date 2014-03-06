# coding: utf-8
import dbapi
import traceback
import sap_hana_credentials as credentials
from folder_iterator import FolderIterator

DATA_DIRECTORY = 'data'
TABLE_NAME = '"DEMOUSER00"."uni.vlba.gdelt.data::gdelt_dailyupdates"'

"""
    Author  : Viktor Dmitriyev
    Goal 	: Establish connection to SAP HANA DB using shipped with SAP HANA python client routine(dpapi) 
    		  and loading GDELT database from daily updates.
    Date    : 06.03.2014
"""

class GDELTDataLoader():

	def __init__(self):
		"""
			Init method.
		"""

		self.connection = self.get_connection()
		fi = FolderIterator()
		self.data_csv_files = fi.iterate_through_catalog(DATA_DIRECTORY)	

	def get_connection(self):
		"""
			(NoneType) -> (Object)

			Method that will return connection to the database using given credentials.
		"""

		return  dbapi.connect(credentials.SERVER,\
							  credentials.PORT,\
							  credentials.USER,\
							  credentials.PASSWORD)

	def build_test_query01(self):
		"""
			(NoneType) -> (str)

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


	def run_query(self, query):		
		"""
			(Connection, str) -> NoneType

			Running given query and using given connection. 
			Fetching result rows and printing them to standard output.
		"""

		cursor = self.connection.cursor()
		executed_cur = cursor.execute(query)
		if executed_cur:
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
			# print value_stripped
			result.append(value_stripped)				
		
		return result


	def escape_data_for_sql(self, value, sql_type):
		"""
			(obj, str) -> str

			Escape symbols to be used in sql statements.
		"""

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



	def form_insert_query(self, table_name, input_data, table_fields_names=None, table_fields_types=None):
		"""
			(obj, )
			Returning insert sql statement
		"""

		# if table_fields is None:

		query_table_structure = '('

		for index in xrange(len(input_data)):			
			query_table_structure = query_table_structure + '"' + table_fields_names[index] + '"' + ','

		query_table_structure = query_table_structure[:len(query_table_structure)-1]
		query_table_structure = query_table_structure + ')'

		# query 
		query_table_values = '('
		for index in xrange(len(input_data)):			
			proper_value = self.escape_data_for_sql(input_data[index], table_fields_types[index])
			query_table_values = query_table_values + proper_value + ','

		query_table_values = query_table_values[:len(query_table_values)-1]
		query_table_values = query_table_values + ')'

		# form query
 		query = 'INSERT INTO ' + table_name + ' ' + query_table_structure + ' VALUES ' + query_table_values

		return query

	def identify_table_mask(self, mask_file_name='daily_update_table-mask.txt', delim=';'):
		"""
			(obj, str) -> (list(), list())

			Extracting table identifiers from the txt file.
			'Table definitions' are taked by simple "copy->paste" from 'Open Definition' of table in SAP HANA Studio.
		"""

		table_fields_names = list()
		table_fields_types = list()		

		mask_f = open(DATA_DIRECTORY + '/' + mask_file_name, "r")
		line = mask_f.readline()
		line = mask_f.readline()
		while line:
			value_list = line.split(delim)
			table_fields_names.append(value_list[0])
			table_fields_types.append(value_list[1])
			line = mask_f.readline()
			
		mask_f.close()

		return table_fields_types, table_fields_names


	def fetch_data_from_csv_and_insert(self):
		"""
			Fetching data from CSV and loading to database.
		"""

		table_fields_types, table_fields_names = self.identify_table_mask()
		
		# print len(table_fields_types)

		for directory in self.data_csv_files:
			for _file in self.data_csv_files[directory]:
				csv_f = open(DATA_DIRECTORY + '/' + _file, "r")
				line = csv_f.readline()
				while line:				
					values_list = self.line_to_list(line)					
					query = self.form_insert_query(TABLE_NAME, values_list, table_fields_names, table_fields_types)
					print query

					# try:
					# 	self.run_query(query)
					# except Exception, e:
					# 	print '[e] Exeption: ' + str(e)

					line = csv_f.readline()				
				csv_f.close()
		
def main():
	"""
		(NoneType) -> NoneType

		Processing .
	"""

	gdl = GDELTDataLoader()
	gdl.fetch_data_from_csv_and_insert()


if __name__ == '__main__':
	main()
