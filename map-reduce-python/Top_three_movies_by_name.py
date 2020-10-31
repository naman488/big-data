from mrjob.step import MRStep
from mrjob.job import MRJob

class TopThreeMovies(MRJob):

    def configure_args(self):
        super(TopThreeMovies, self).configure_args()
        self.add_file_arg('--items', help='C:/Users/bigdata/u.item')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                    reducer_init=self.reducer_init,
                    reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_find_top)
        ]

    def mapper_get_ratings(self, key, value):
        (userID, movieID, rating, timestamp) = value.split('\t')
        yield movieID, 1

    def reducer_init(self):
        self.movieNames = {}

        with open("C:/Users/bigdata/u.item") as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1]

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), self.movieNames[key])

    def reducer_find_max(self, key, values):
        yield max(values)
        
    def reducer_find_top(self, key, values):
        top = sorted(values, reverse=True)
        for x in top[0:3]:
            yield x

if __name__ == '__main__':
    TopThreeMovies.run()