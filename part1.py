
import mrjob
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+") 

class MRMostUsedWord(MRJob):
	pass

	def mapper(self, _, line):
		for i in WORD_RE.findall(line):
			yield i.lower(), 1
			
	def combiner(self, word, counts):
		yield word, sum(counts)

	def reducer(self, word, counts):
		yield word, sum(counts)




if __name__ == "__main__":
	MRMostUsedWord.run()
