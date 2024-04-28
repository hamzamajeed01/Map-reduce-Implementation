
from mrjob.job import MRJob
import re

class AuthorsWithOnePaperPerYear(MRJob):

    def mapper_init(self):
        self.author_paper_count = {}

    def mapper(self, _, line):
        author_match = re.search(r'#@\s*(.*)#t(\d+)', line)
        if author_match:
            authors = author_match.group(1).split(',')
            year = int(author_match.group(2))
            if year not in self.author_paper_count:
                self.author_paper_count[year] = {}
            for author in authors:
                author = author.strip()
                self.author_paper_count[year][author] = self.author_paper_count[year].get(author, 0) + 1

    def mapper_final(self):
        for year, author_count_dict in self.author_paper_count.items():
            for author, count in author_count_dict.items():
                yield (year, author), count

    def reducer(self, author_year, counts):
        total_papers = sum(counts)
        if total_papers <= 1:
            yield author_year[0], author_year[1]

if __name__ == '__main__':
    AuthorsWithOnePaperPerYear.run()
       
