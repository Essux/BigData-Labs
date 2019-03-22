from datetime import datetime

def parse_line(line):
    line = line.split(',')
    return {
        'user' : int(line[0]),
        'movie' : int(line[1]),
        'rating' : int(line[2]),
        'genre' : line[3],
        'date' : line[4]
    }

def parse_date(date):
    return datetime.strptime(date, '%Y-%m-%d')