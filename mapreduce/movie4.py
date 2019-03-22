# Número de usuarios que ven una misma película y el rating promedio
from mrjob.job import MRJob, MRStep
from utils import parse_line

class Movie(MRJob):

    def mapper(self, _, line):
        line = parse_line(line)
        yield line['movie'], line['rating']

    def reducer(self, key, values):
        values = list(values)
        yield key, (len(values), sum(values)/len(values))


if __name__ == '__main__':
    Movie.run()