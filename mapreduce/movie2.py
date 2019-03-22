# Día en que más películas se han visto
from mrjob.job import MRJob, MRStep
from utils import parse_line

class Movie(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_dates),
            MRStep(reducer=self.reducer_max_date)
        ]

    def mapper(self, _, line):
        line = parse_line(line)
        yield line['date'], 1

    def reducer_dates(self, date, values):
        yield None, (date, sum(values))

    def reducer_max_date(self, key, values):
        yield None, max(values, key=lambda x : x[1])


if __name__ == '__main__':
    Movie.run()