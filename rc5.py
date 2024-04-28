from mrjob.job import MRJob
import re

class Part5(MRJob):

    def mapper_init(self):
        self.author_counts = {}

    def mapper(self, _, line):
        author_match = re.search(r'#@\s*(.*)#t', line)
        if author_match:
            authors = author_match.group(1).split(',')
            for author in authors:
                author = author.strip()
                if author not in self.author_counts:
                    self.author_counts[author] = 0
                self.author_counts[author] += 1

    def mapper_final(self):
        for author, count in self.author_counts.items():
            yield None, (author, count)

    def reducer(self, _, author_counts):
        author_counts = list(author_counts)

        max_papers = 0
        max_authors = []

        for author, count in author_counts:
            if author:  
                if count > max_papers:
                    max_papers = count

        for author, count in author_counts:
            if count == max_papers and author:  
                max_authors.append(author)

        if max_authors:
            for author in max_authors:
                yield "Author(s) with the maximum number of papers:", author
        else:
            yield "No authors found with the maximum number of papers:", ""

if __name__ == '__main__':
    Part5.run()

   

