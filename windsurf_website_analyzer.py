import json
from collections import defaultdict
from groq_query import GroqQuery
from cache import Cache
from tavily_particular_website_search import TavilyParticularWebsiteSearch
from tqdm import tqdm
from windsurf_finder import WindsurfFinder
import asyncio
from concurrent.futures import ThreadPoolExecutor

class WindsurfWebsiteAnalyzer:
    def __init__(self):
        self.tavily_website_search = TavilyParticularWebsiteSearch()
        self.groq_query = GroqQuery()
        self.cache = Cache(tool="windsurf_website_analyzer")

    def analyze_websites(self, windsurf_finder_results):
        if not windsurf_finder_results:
            print("No windsurf finder results provided.")
            return {}
        all_results = {}
        
        async def process_domains_async(domains):
            with ThreadPoolExecutor(max_workers=1) as executor:
                loop = asyncio.get_event_loop()
                tasks = [loop.run_in_executor(executor, self._process_domain, domain, urls) for domain, urls in domains.items()]
                
                for completed_task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Analyzing websites"):
                    domain, subpage_results = await completed_task
                    if subpage_results:
                        all_results[domain] = subpage_results
        
        asyncio.run(process_domains_async(windsurf_finder_results))
        return all_results


    def _process_domain(self, domain, urls):
        print(f"Processing domain: {domain}")
        cache_key = f"website_analysis_{domain}"
        cached_result = self.cache.get(cache_key)
        if cached_result:
            print(f"  - Returning cached analysis for {domain}")
            return domain, cached_result
        
        tavily_results = self.tavily_website_search.search(domain)
        if not tavily_results or not tavily_results.get('results'):
            print(f"  - No Tavily results found for domain: {domain}")
            return domain, {}
        
        subpage_results = self._categorize_subpages(domain, tavily_results['results'])
        self.cache.set(cache_key, subpage_results)
        return domain, subpage_results

    def _categorize_subpages(self, domain, results):
        categories = ["location_information", "pricing", "camps", "courses", "weather_conditions", "transport_options", "other"]
        subpage_results = defaultdict(list)
        for result in tqdm(results, desc=f"  - Categorizing subpages for {domain}"):
            url = result.get('url')
            title = result.get('title', '')
            description = result.get('description', '')
            if url:
                cache_key_title = ''.join(filter(str.isalnum, title[:10])).lower()
                query_text = f"{title} {description}"
                cache_key = f"{domain}_{cache_key_title}"
                cached_groq_result = self.cache.get(cache_key)
                if cached_groq_result:
                    groq_result_json = cached_groq_result
                else:
                    groq_result = self.groq_query.query(query_text, categories)
                    if groq_result:
                        groq_result_json = json.loads(groq_result)
                        self.cache.set(cache_key, groq_result_json)
                    else:
                        continue
                
                for category in categories:
                    if category != "other" and groq_result_json.get(category, 0) > 0.3:
                        subpage_results[category].append(url)
        return subpage_results

    def _get_windsurf_finder_results(self, area):
        finder = WindsurfFinder()
        return finder.find_windsurf_locations(area)

if __name__ == '__main__':
    analyzer = WindsurfWebsiteAnalyzer()
    area = "Lanzarote"
    windsurf_finder_results = analyzer._get_windsurf_finder_results(area)
    website_analysis = analyzer.analyze_websites(windsurf_finder_results)
    if website_analysis:
        for domain, subpages in website_analysis.items():
            print(f"Domain: {domain}")
            for category, urls in subpages.items():
                print(f"  Category: {category}")
                for url in urls:
                    print(f"    - {url}")
    else:
        print("No website analysis found.")
