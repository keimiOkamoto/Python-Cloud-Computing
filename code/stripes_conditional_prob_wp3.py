
from mrjob.job import MRJob
import re

WORD_OF_INTEREST = "for"
words_after = {}


#END OF DOCUMENT REGEX
#
class StripesProbability(MRJob):
	last_word = "" 
	first = True
	paragraph = ""
	
	def cleanDoc(self, _, line):
		if (line.strip() != ""):
			self.paragraph += line
			
		else: 
			words = re.compile(r"[\w']+").findall(self.paragraph.lower())
			yield (1,words)
	
	def mapper(self, _, line):
		#words = re.compile(r"[\w']+").findall(line.lower())
		print "XXXXXXXXXXXXXXXXXXxx" 
		print line
		'''
		for index, word in enumerate(words):
			if word == WORD_OF_INTEREST:
				if(index != len(words)-1):
					if words[index + 1] not in words_after:
						words_after[words[index+1]] = 1
					else:
						words_after[words[index+1]] += 1
				else:
					self.last_word = word
		if words :		
			if self.last_word !=  WORD_OF_INTEREST and self.first != True:
				if words[0] in words_after:
					words_after[words[0]] += 1
				else:
				 	words_after[words[0]] = 1
				self.last_word = ""	

		self.first = False
		print words_after
		'''
	
	def reducer(self, word, count):
		yield (word, sum(count))
		
	def reducer2(self, count, words):
		yield (word, sum(count))
			
	def steps(self):
		return [
			self.mr(mapper=self.cleanDoc),
			self.mr(
				mapper=self.mapper,
				#combiner=self.combiner,
				reducer=self.reducer),
			self.mr(reducer=self.reducer2)
			#self.mr(reducer=self.thetop)
		]
			
		
if __name__ == '__main__':
	StripesProbability.run()
