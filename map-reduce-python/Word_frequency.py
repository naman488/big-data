from mrjob.job import MRJob
import re

regexp = re.compile(r"[\w']+")

class WordFrequency(MRJob):

    def mapper(self, _, line):
        words = regexp.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    WordFrequency.run()
