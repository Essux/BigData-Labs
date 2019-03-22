# Numero de peliculas vista por un usuario, valor promedio de calificacion
from mrjob.job import MRJob
from utils import parse_line

class Movie(MRJob):

    def mapper(self, _, line):
        line = parse_line(line)
        yield line['user'], line['rating']

    def reducer(self, key, values):
        values = list(values)
        yield key, (len(values), sum(values)/len(values))


if __name__ == '__main__':
    Movie.run()