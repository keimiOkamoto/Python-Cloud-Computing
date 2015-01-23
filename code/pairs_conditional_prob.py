from mrjob.job import MRJob
import re

WORD_OF_INTEREST = "for"

class PairsProbability(MRJob):

	last_word = ""
	pair=[] 
	topten = [0]*10
	
	#Mapper that splits words in the cocument to pairs along with a count
	#A special character '*' is user to signify that only one reducer be used.
	def mapper(self, _, line):
		words = re.compile(r"[\w']+").findall(line.lower())
		
		if words:
			if self.last_word:
				pair = self.last_word, words[0]
				yield [pair[0],("*", 1)]
				yield [pair[0], (pair[1], 1)]
			
			for element in range(len(words)): 
				if element != len(words)-1:
					pair = words[element], words[element+1]
					yield [pair[0],("*", 1)]
					yield [pair[0], (pair[1], 1)]
					
			
			self.last_word = words[len(words)-1]
		else:
			self.last_word = ""
		
	
	def reducer(self, word, count):
		total_word_of_interest = 0.0;
		words_after = {} #Dictionary to store pairs for the word for
		
		if word == WORD_OF_INTEREST:
			for element in count:
				if element[0] == "*":
					total_word_of_interest += 1
				else :
					if element[0] not in words_after:
						words_after[element[0]] = 1
					else: 
						words_after[element[0]]+= 1
					
		yield (total_word_of_interest, words_after)
	

	def reducer_probability_calculator(self, count, words):
		for wordz in words:
			for word, occur in wordz.items():
				yield word, occur/count
				
	def topTenDecending(self, word, probability):
		for num in probability :
			for index in range(1,11):
				if num >= self.topten[index]:
					self.topten.insert(index,(num,word))
					break
					
		decending = sorted(self.topten, reverse =True)
		
		for count in range(10):
			print decending[count]
				
	def steps(self):
		return [
			self.mr(mapper=self.mapper,
				reducer=self.reducer),
			self.mr(reducer=self.reducer_probability_calculator),
			self.mr(reducer=self.topTenDecending)
		]
			
		
if __name__ == '__main__':
	PairsProbability.run()
