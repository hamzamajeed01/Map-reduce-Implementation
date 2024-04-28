from mrjob.job import MRJob
import re
class Part2(MRJob):

    def mapper_init(self):
        self.year_titles = {}

    def mapper(self, _, line):
        
        year_match = re.search(r'#t(\d{4})', line)  
        title_match = re.search(r'(.+)(?=#@)', line) 
        if year_match and title_match:
            year = int(year_match.group(1))
            title = title_match.group(1).strip().lstrip('#*')
            if year not in self.year_titles:
                self.year_titles[year] = []
            self.year_titles[year].append(title)

    def mapper_final(self):
        for year, titles in self.year_titles.items():
            yield year, titles
    def reducer(self, year, titles_lists):
        combined= []
        for titles in titles_lists:
            for title in titles:
                combined.append(title)
        combined_titles_str = ' , '.join(combined)
        yield year, combined_titles_str
if __name__ == '__main__':
    Part2.run()
