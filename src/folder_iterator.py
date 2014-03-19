# coding: utf-8
import os

"""
    Author  : Viktor Dmitriyev
    Goal 	: Helper script that iterates through specified folder and extracts all file names.
   	Note   	: 
    Date    : 30.07.2013
"""

class FolderIterator():

	def iterate_through_catalog(self, rootdir=None):
		""" (str) -> (dict, dict)
		
			Iterating through the given catalog to identify notes.
		"""

		if rootdir is None:
			rootdir = sys.argv[1]

		# notes_papers = dict()
		total_papers = dict()

		for root, _, files in os.walk(rootdir):		
			for f in files:
				# if self.is_publication(f):
				if (f[-4:].lower() in ('.csv')):
					if root not in total_papers:
						total_papers[root] = list()
					total_papers[root].append(f)

		return total_papers