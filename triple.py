"""Find the URLs triples. program which finds all paths of length two in the
corresponding URL links. That is, it finds the triples of URLs (u, v, w) such that there is a link
from u to v and a link from v to w

"""

from mrjob.job import MRJob
import csv


def csv_readlines(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class UrlsTriples(MRJob): 	
	def mapper(self, url, line):
		# read line by line, elements are seperated by comma.
		line = line.split(',')
		#yield key/value 
		yield url, line

	def reducer (self, URL, links):	
		""" converting the generator links to tuples"""
		links= tuple(links)
		title = "The triples of URLs (u, v, w) = " # Naming the title of my output.
		"""Used two for loops to iterate over the  tuple in links, 
		if the destination(v) in one tuple matched the source(u) in the other,
		concatenate the two tuples forming the triples(u,v,w) """
		for destination in links:
			for source in links:
				if destination[1]==source[0]:
					triples=destination+source[1:]
					#join the urls links by ","
					triples=', '.join([str(i) for i in triples]) 
					yield title, triples
					
					

if __name__ == '__main__':
	UrlsTriples.run()