from mrjob.job import MRJob
import re
#import heapq

WORD_OF_INTEREST = "for"

class PairsProbability(MRJob):

	last_word = ""
	pair=[] 
	x = 0
	total_wrd_ct = 0.0
	
	def mapper(self, _, line):
		words = re.compile(r"[\w']+").findall(line.lower())
		
		if words:
			if self.last_word:
				pair = self.last_word, words[0]
				print pair
				yield [pair[0], ("*", 1)] #check if the 1 can be knocked off
				yield [pair, 1]
			
			for element in range(len(words)): 
				if element != len(words)-1:
					pair = words[element], words[element+1]
					print pair
					yield [pair[0],("*") 1]
					yield [pair, 1]
			
			self.last_word = words[len(words)-1]
		else:
			self.last_word = ""
		

	def reducer(self, word, count): #out key value
		#code
			
	def steps(self):
		return [
			self.mr(mapper=self.mapper,
				#combiner=self.combiner,
				reducer=self.reducer),
			#self.mr(reducer=self.thetop)
		]
			
		
if __name__ == '__main__':
	PairsProbability.run()
