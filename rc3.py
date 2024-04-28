from mrjob.job import MRJob
import re

class Part3(MRJob):
    def mapper_init(self):
        self.co_authors= {}
    def mapper(self, _, line):
        authors_match = re.search(r'#@\s*(.*)#t', line)
        if authors_match:
            authors = authors_match.group(1).split(',')
            for author in authors:
                author = author.strip()
                if author not in self.co_authors:
                    self.co_authors[author] = set()
                self.co_authors[author].update(set(authors) - {author})   
    def mapper_final(self):
        for author, co_authors in self.co_authors.items():
            if author:
                yield author, list(co_authors)
    def reducer(self, author, co_authors_lists):
        co_authors = set()
        for co_authors_list in co_authors_lists:
            co_authors.update(co_authors_list)
        yield author, list(co_authors)

if __name__ == '__main__':
    Part3.run()

