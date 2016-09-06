# coding: utf-8
#!/usr/bin/env python

__author__      = "Viktor Dmitriyev"
__copyright__   = "Copyright 2016, Viktor Dmitriyev"
__credits__     = ["Viktor Dmitriyev"]
__license__     = "MIT"
__version__     = "1.0.0"
__email__       = ""
__status__      = "test"
__date__        = "30.07.2013"
__description__ = "Helper script that iterates through specified folder and extracts all file names."

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
