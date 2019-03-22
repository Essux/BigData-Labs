# La mejor y peor pelicula evaluada por genero
from mrjob.job import MRJob
from utils import parse_line

class Movie(MRJob):

    def mapper(self, _, line):
        line = parse_line(line)
        yield line['genre'], (line['movie'], line['rating'])

    def reducer(self, key, values):
        values = list(values)
        best = max(values, key=lambda x: x[1])
        worst = min(values, key=lambda x: x[1])
        yield key, (best, worst)


if __name__ == '__main__':
    Movie.run()