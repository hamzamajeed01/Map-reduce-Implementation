from mrjob.job import MRJob
import re

class Part7(MRJob):

    def mapper_init(self):
        self.titles = []

    def mapper(self, _, line):
        title_match = re.search(r'#\*\s*(.*)\s*#@', line)
        venue_match = re.search(r'#c\s*(.*)', line)
        if title_match and not venue_match:
            title = title_match.group(1)
            self.titles.append(title)

    def mapper_final(self):
        for title in self.titles:
            yield None, title

    def reducer_init(self):
        self.titles = []

    def reducer(self, _, titles):
        for title in titles:
            self.titles.append(title)

    def reducer_final(self):
        for title in self.titles:
            yield title,None

if __name__ == '__main__':
    Part7.run()

