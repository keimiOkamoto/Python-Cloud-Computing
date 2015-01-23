
from mrjob.job import MRJob
import re

WORD_OF_INTEREST = "for"
words_after = {}

class StripesProbability(MRJob):

	def mapper(self, _, line):
		total= []
		words = re.compile(r"[\w']+").findall(line.lower())
		
		for index, word in enumerate(words):
			if word == WORD_OF_INTEREST:
				if(index != len(words)-1):
					if words[index + 1] not in words_after:
						words_after[words[index+1]] = 1
					else:
						words_after[words[index+1]] += 1
		
		print words_after
	
	
	def reducer(self, word, count):
		yield (word, sum(count))
		
	def reducer2(self, count, words):
		yield (word, sum(count))
			
	def steps(self):
		return [
			self.mr(mapper=self.mapper,
				#combiner=self.combiner,
				reducer=self.reducer),
			self.mr(reducer=self.reducer2)
			#self.mr(reducer=self.thetop)
		]
			
		
if __name__ == '__main__':
	StripesProbability.run()
