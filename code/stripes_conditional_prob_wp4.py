
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
import re

WORD_OF_INTEREST = "for"

#END OF DOCUMENT REGEX

class StripesProbability(MRJob):
	last_word = "" 
	first = True
	paragraph = ""
	TOTAL_COUNT = 0
	global words_after
	global words
	words_after ={}
	#OUTPUT_PROTOCOL = pickleProtocol

	def mapper(self, _, line):
		words = []
		if line.strip() != "":
			words = re.compile(r"[\w']+").findall(line.lower())
		
		if words :
			for index, word in enumerate(words):
				if word == WORD_OF_INTEREST:
					if(index != len(words)-1):
						if words[index + 1] not in words_after:
							words_after[words[index+1]] = 1
						else:
							words_after[words[index+1]] += 1
					else:
						self.last_word = word
						
			if self.last_word !=  WORD_OF_INTEREST and self.first != True:
				if words[0] in words_after:
					words_after[words[0]] += 1
				else:
				 	words_after[words[0]] = 1
				self.last_word = ""	

			self.first = False
		else :
			if words_after:
				print "yeilded   "
				print words_after
				
				#array = ["this",1,2,3]
				
				yield (WORD_OF_INTEREST, words_after)
				words_after.clear()
	
	def reducer(self, words, dic):
		stripes = {}
		
		for element in dic:
			for k, v in element.items():
				if k in stripes:
					stripes[k]+= v
				else:
					stripes[k] = v
				self.TOTAL_COUNT +=v
		
		
		print stripes
		x = [(stripes[i],i) for i in stripes.keys()]
		x.sort() 
		print x
		yield (self.TOTAL_COUNT, x[-10:])
	
	#add array of 10 , sort on value
	def reducer2(self, count, dic):
		for element in dic:
			for (k, v) in element:
				print v, float(k)/count

		
		
			
	def steps(self):
		return [
			self.mr(
				mapper=self.mapper,
				#combiner=self.combiner,
				reducer=self.reducer),
			self.mr(reducer=self.reducer2)
			#self.mr(reducer=self.thetop)
		]
			
		
if __name__ == '__main__':
	StripesProbability.run()
