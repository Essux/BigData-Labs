# Dia en que mejor evaluacion han dado los usuarios
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
        yield line['date'], line['rating']

    def reducer_dates(self, date, values):
        values = list(values)
        yield None, (date, sum(values)/len(values))

    def reducer_max_date(self, key, values):
        yield None, max(values, key=lambda x : x[1])


if __name__ == '__main__':
    Movie.run()