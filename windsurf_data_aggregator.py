import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from groq_query import GroqQuery
from cache import Cache
from tqdm import tqdm
from collections import defaultdict
from windsurf_website_analyzer import WindsurfWebsiteAnalyzer

class WindsurfDataAggregator:
    def __init__(self):
        self.groq_query = GroqQuery()
        self.cache = Cache(tool="windsurf_data_aggregator")
        self.structured_output_format = {
            "location_information": {
                "name": None,
                "city": None,
                "contact_details": {
                    "phone": None,
                    "email": None
                },
                "comments": None
            },
            "pricing": {
                "windsurfing": {
                    "hourly_rate": None,
                    "daily_rate": None,
                    "package_3_to_7_days": None
                },
                "surfing": {
                    "availability": None,
                    "hourly_rate": None,
                    "daily_rate": None
                },
                "equipment_rental": {
                    "included_in_pricing": None,
                    "rental_rate_per_hour": None,
                    "rental_rate_per_day": None
                },
                "equipment_insurance": {
                    "included": None,
                    "cost_per_day": None
                },
                "comments": None
            },
            "courses": [],
            "transport_options": {
                "pickup_service": {
                    "availability": None,
                    "cost": None,
                    "from_airport": None,
                    "from_city_center": None
                }
            }
        }

    async def _fetch_text_from_url(self, url):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    text = soup.get_text(separator=' ', strip=True)
                    return text
        except aiohttp.ClientError as e:
            print(f"Error fetching URL {url}: {e}")
            return None
        except Exception as e:
            print(f"Error processing URL {url}: {e}")
            return None

    def _extract_data_from_text(self, text):
        categories = ["location_information", "pricing", "courses", "transport_options"]
        prompt = f"""
        Given the following text, extract the information and return a JSON object using the following format:
        {json.dumps(self.structured_output_format)}
        Text: {text}
        """
        try:
            groq_result = self.groq_query.query(prompt, categories)
            if groq_result:
                return json.loads(groq_result)
            else:
                return None
        except Exception as e:
            print(f"Error during Groq query: {e}")
            return None

    async def _process_subpage(self, url):
        cache_key = f"subpage_content_{url}"
        cached_result = self.cache.get(cache_key)
        if cached_result:
            return cached_result
        
        text = await self._fetch_text_from_url(url)
        if text:
            extracted_data = self._extract_data_from_text(text)
            if extracted_data:
                self.cache.set(cache_key, extracted_data)
                return extracted_data
        return None

    def aggregate_data(self, website_analysis):
        aggregated_results = {}
        for domain, categories in website_analysis.items():
            aggregated_results[domain] = self._aggregate_domain_data(domain, categories)
        return aggregated_results

    def _aggregate_domain_data(self, domain, categories):
        aggregated_data = self.structured_output_format.copy()
        
        async def process_categories_async(categories):
            tasks = []
            for category, urls in categories.items():
                for url in urls:
                    tasks.append(self._process_subpage(url))
            
            subpage_results = await asyncio.gather(*tasks)
            
            for result in subpage_results:
                if result:
                    self._merge_data(aggregated_data, result)
        
        asyncio.run(process_categories_async(categories))
        return aggregated_data

    def _merge_data(self, aggregated_data, new_data):
        def _recursive_merge(agg_data, new_data):
            if isinstance(agg_data, dict) and isinstance(new_data, dict):
                for key, new_value in new_data.items():
                    if key not in agg_data or agg_data[key] is None:
                        agg_data[key] = new_value
                    else:
                        _recursive_merge(agg_data[key], new_value)
            elif isinstance(agg_data, list) and isinstance(new_data, list):
                 agg_data.extend(new_data)
            
        _recursive_merge(aggregated_data, new_data)

if __name__ == '__main__':
    analyzer = WindsurfWebsiteAnalyzer()
    area = "Lanzarote"
    windsurf_finder_results = analyzer._get_windsurf_finder_results(area)
    website_analysis = analyzer.analyze_websites(windsurf_finder_results)
    
    aggregator = WindsurfDataAggregator()
    aggregated_data = aggregator.aggregate_data(website_analysis)
    print(json.dumps(aggregated_data, indent=4))
