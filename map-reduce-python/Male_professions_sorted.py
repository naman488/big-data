from mrjob.job import MRJob
from mrjob.step import MRStep

class Top_male_profession(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_profession_name,
                    reducer=self.reducer_profession_count),
            MRStep(reducer=self.reducer_find_top)]

    def mapper_profession_name(self, _, line):
        (number, age, type, profession, y) = line.split('|')
        if (type == 'M'):
            yield profession, 1

    def reducer_profession_count(self,key_profession,count):
        yield None, (sum(count), key_profession)
        
    def reducer_find_top(self, key, sum_count):
        top = sorted(sum_count, reverse=True)
        for x in top:
            yield x
        
if __name__ == '__main__':
    Top_male_profession.run()
