import os
from tavily import TavilyClient
from cache import Cache
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

class TavilyParticularWebsiteSearch:
    def __init__(self):
        self.api_key = os.getenv("TAVILY_API_KEY")
        if not self.api_key:
            raise ValueError("TAVILY_API_KEY not found in environment variables")
        self.client = TavilyClient(api_key=self.api_key)
        self.cache = Cache(tool="tavily_particular_website")

    def search(self, domain, max_results=10):
        query = f"windsurfing school, rental, camp, pricing, courses, lessons, equipment"
        cache_key = f"{domain}_{query}_{max_results}"
        cached_result = self.cache.get(cache_key)
        if cached_result:
            print(f"Returning cached result for {domain}")
            return cached_result
        
        print(f"Fetching new result for {domain}")
        response = self.client.search(query, max_results=max_results, include_domains=[domain])
        self.cache.set(cache_key, response)
        return response

if __name__ == '__main__':
    search_tool = TavilyParticularWebsiteSearch()
    domain = "www.clublasanta.com"
    results = search_tool.search(domain)
    print(results)
    
    # Test cache
    results = search_tool.search(domain)
    print(results)
