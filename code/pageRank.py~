from mrjob.job import MRJob
from mrjob.protocol import RawProtocol

WORD_OF_INTEREST = "for"

class SNAP_PageRank(MRJob):

	INPUT_PROTOCOL = RawProtocol
	
	#Mapper that splits words in the cocument to pairs along with a count
	#A special character '*' is user to signify that only one reducer be used.
	def mapper(self, node, link):
		print node
		if node[0] == '#':
			return
		yield node, link
	
	def reducer(self, word, count):
		yield(count(sum))
				
	def steps(self):
		return [
			self.mr(mapper=self.mapper,
				reducer=self.reducer),
		]
			
		
if __name__ == '__main__':
	SNAP_PageRank.run()
