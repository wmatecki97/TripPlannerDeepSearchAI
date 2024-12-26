import os
from tavily import TavilyClient
from cache import Cache
from dotenv import load_dotenv

load_dotenv()

class TavilySearch:
    def __init__(self):
        self.api_key = os.getenv("TAVILY_API_KEY")
        if not self.api_key:
            raise ValueError("TAVILY_API_KEY not found in environment variables")
        self.client = TavilyClient(api_key=self.api_key)
        self.cache = Cache()

    def search(self, query, max_results=10):
        cache_key = f"{query}_{max_results}"
        cached_result = self.cache.get(cache_key)
        if cached_result:
            print("Returning cached result")
            return cached_result
        
        print("Fetching new result")
        response = self.client.search(query, max_results=max_results)
        self.cache.set(cache_key, response)
        return response

if __name__ == '__main__':
    search_tool = TavilySearch()
    search_phrase = "Who is Leo Messi?"
    results = search_tool.search(search_phrase)
    print(results)
    
    # Test cache
    results = search_tool.search(search_phrase)
    print(results)
