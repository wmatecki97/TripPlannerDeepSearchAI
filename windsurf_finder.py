from tavily_search import TavilySearch
from urllib.parse import urlparse
from collections import defaultdict

class WindsurfFinder:
    def __init__(self):
        self.search_tool = TavilySearch()

    def find_windsurf_locations(self, area):
        query = f"windsurf schools or shops in {area}"
        results = self.search_tool.search(query, max_results=200)
        
        if not results or not results.get('results'):
            print("No results found.")
            return []
        
        domains = self._analyze_results(results['results'])
        return domains

    def _analyze_results(self, results):
        domains = defaultdict(list)
        for result in results:
            url = result.get('url')
            if url:
                parsed_url = urlparse(url)
                domain = parsed_url.netloc
                domains[domain].append(url)
        
        for domain, urls in domains.items():
            domains[domain] = sorted(urls, key=len)
        
        return domains

if __name__ == '__main__':
    finder = WindsurfFinder()
    area = "Lanzarote"
    locations = finder.find_windsurf_locations(area)
    if locations:
        for domain, urls in locations.items():
            print(f"Domain: {domain}")
            for url in urls:
                print(f"  - {url}")
    else:
        print("No windsurf locations found.")
