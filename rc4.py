from mrjob.job import MRJob
import re

class PaperCount(MRJob):

    def mapper_init(self):
        self.publish_counts = {}
    def mapper(self, _, line):
        year_match = re.search(r'#t(\d+)', line)
        if year_match:
            year = int(year_match.group(1))
            if year not in self.publish_counts:
                self.publish_counts[year] = 0
            self.publish_counts[year] += 1

    def mapper_final(self):
        for year, count in self.publish_counts.items():
            yield year, (count,1)

    def reducer(self, year, counts):
        total_papers = 0
        total_years = 0
        for count,i in counts:
            total_years += 1
            total_papers += count
        avg = total_papers / total_years
        yield year, avg

if __name__ == '__main__':
    PaperCount.run()
