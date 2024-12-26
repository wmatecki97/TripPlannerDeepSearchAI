import json
from tavily_search import TavilySearch
from urllib.parse import urlparse
from collections import defaultdict
from groq_query import GroqQuery

class WindsurfFinder:
    def __init__(self):
        self.search_tool = TavilySearch()
        self.groq_query = GroqQuery()

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
        filtered_domains = {}
        for result in results:
            url = result.get('url')
            if url:
                parsed_url = urlparse(url)
                domain = parsed_url.netloc
                domains[domain].append(result)
        
        for domain, results in domains.items():
            sorted_results = sorted(results, key=lambda x: len(x.get('url', '')))
            first_result = sorted_results[0]
            title = first_result.get('title', '')
            description = first_result.get('description', '')
            
            query_text = f"{title} {description}"
            categories = ["windsurf_related_website", "other_website"]
            
            try:
                groq_result = self.groq_query.query(query_text, categories)
                if groq_result:
                    groq_result_json = json.loads(groq_result)
                    windsurf_related_probability = groq_result_json.get("windsurf_related_website", 0)
                    other_website_probability = groq_result_json.get("other_website", 0)
                    if windsurf_related_probability > other_website_probability:
                        filtered_domains[domain] = [res.get('url') for res in sorted_results]
            except Exception as e:
                print(f"Error processing domain {domain}: {e}")
        
        return filtered_domains

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
