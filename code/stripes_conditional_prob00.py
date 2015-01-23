
from mrjob.job import MRJob
import re

WORD_OF_INTEREST = "for"
words_after = {}


#END OF DOCUMENT REGEX
# (word, (adjWord, count))
class StripesProbability(MRJob):
	last_word = "" 
	first = True
	paragraph = ""
	
	def mapper(self, _, line):
		words = re.compile(r"[\w']+").findall(line.lower())
		if words :
			for index, word in enumerate(words):
				if(index != len(words)-1):
					if words[index + 1] not in words_after:
						words_after(word, (words[index+1], 1)
					else:
						words_after[word, (words[index+1], 1)] += 1
				else:
					self.last_word = word
			if words :		
				self.first != True:
					if words[0] in words_after:
						words_after[self.last_word, (words[0], 1)] += 1
					else:
					 	words_after[words[0]] = 1
					self.last_word = ""	

			self.first = False
		
		for word in words_after:
			print word
			yield (word, words_after.get(word))
		print words_after
	
	def reducer(self, word, counts):
		total = 0
		
		print "xxxxxxxxx"
		if counts :
			for count in counts:
				print "_________________"
				print word
				print count
				print "_________________"
				total += count
			yield word, total
				
		#yield (word, sum(count))
		
			
	def steps(self):
		return [
			self.mr(
				mapper=self.mapper,
				#combiner=self.combiner,
				reducer=self.reducer),
			#self.mr(reducer=self.reducer2)
			#self.mr(reducer=self.thetop)
		]
			
		
if __name__ == '__main__':
	StripesProbability.run()
