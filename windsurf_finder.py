import json
from tavily_search import TavilySearch
from urllib.parse import urlparse
from collections import defaultdict
from groq_query import GroqQuery
from cache import Cache
from tqdm import tqdm

class WindsurfFinder:
    def __init__(self):
        self.search_tool = TavilySearch()
        self.groq_query = GroqQuery()
        self.cache = Cache(tool="windsurf_finder")

    def find_windsurf_locations(self, area):
        query = f"windsurf schools or shops in {area}"
        results = self.search_tool.search(query, max_results=200)
        
        if not results or not results.get('results'):
            print("No results found.")
            return []
        
        print(f"Found {len(results['results'])} results, analyzing domains...")
        domains = self._analyze_results(results['results'])
        print(f"Analysis complete, found {len(domains)} domains.")
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
        
        def process_domain(domain, results):
            sorted_results = sorted(results, key=lambda x: len(x.get('url', '')))
            first_result = sorted_results[0]
            title = first_result.get('title', '')
            description = first_result.get('description', '')
            
            cache_key_title = ''.join(filter(str.isalnum, title[:10])).lower()
            query_text = f"{title} {description}"
            cache_key = f"{domain}_{cache_key_title}"
            cached_result = self.cache.get(cache_key)
            if cached_result:
                return domain, cached_result
            categories = ["windsurf_rental_or_school", "windsurfing_magazine", "sport_complex", "holiday_center", "other"]
            
            try:
                groq_result = self.groq_query.query(query_text, categories)
                if groq_result:
                    groq_result_json = json.loads(groq_result)
                    windsurf_rental_or_school_probability = groq_result_json.get("windsurf_rental_or_school", 0)
                    sport_complex_probability = groq_result_json.get("sport_complex", 0)
                    holiday_center_probability = groq_result_json.get("holiday_center", 0)

                    if windsurf_rental_or_school_probability > 0.5 or sport_complex_probability > 0.5 or holiday_center_probability > 0.5:
                        urls = [res.get('url') for res in sorted_results]
                        self.cache.set(cache_key, urls)
                        return domain, urls
            except Exception as e:
                print(f"Error processing domain {domain}: {e}")
            return domain, None
        
        
        tasks = []
        for domain, results in tqdm(domains.items(), desc="Processing domains"):
            tasks.append(process_domain(domain, results))
        
        for domain, urls in tasks:
            if urls:
                filtered_domains[domain] = urls
        
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
