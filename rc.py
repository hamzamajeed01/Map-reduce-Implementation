from mrjob.job import MRJob
import re

class Part1(MRJob):
    def mapper_init(self):
        self.decades = {}

    def mapper(self, _, line):
        year = re.findall(r'#t(\d+)', line)
        if year:
            year = int(year[0])
            decade = (year // 10) * 10
            if decade not in self.decades:
                self.decades[decade] = 0
            self.decades[decade] += 1

    def mapper_final(self):
        for decade, count in self.decades.items():
            yield decade, count
    def reducer(self, decade, counts):
        yield decade, sum(counts)

if __name__ == '__main__':
    Part1.run()
