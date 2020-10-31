from mrjob.job import MRJob

class RatingCount(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer(self, rating, value):
        yield rating, sum(value)

if __name__ == '__main__':
    RatingCount.run()
