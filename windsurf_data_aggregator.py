import json
import aiohttp
from bs4 import BeautifulSoup
from groq_structured_query import GroqStructuredQuery
from cache import Cache
from tqdm import tqdm
from collections import defaultdict
from windsurf_website_analyzer import WindsurfWebsiteAnalyzer
import asyncio

class WindsurfDataAggregator:
    def __init__(self):
        self.groq_query = GroqStructuredQuery()
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
        try:
            groq_result = self.groq_query.query(text, self.structured_output_format)
            return groq_result
        except Exception as e:
            print(f"Error during Groq query: {e}")
            return None

    def _process_subpage(self, url, aggregated_data):
        print(f"  Processing subpage: {url}")
        cache_key = f"subpage_content_{url}"
        cached_result = self.cache.get(cache_key)
        if cached_result:
            print(f"    - Cache hit for {url}")
            self._merge_data(aggregated_data, cached_result)
            return
        
        text =  asyncio.run(self._fetch_text_from_url(url))
        if text:
            extracted_data = self._extract_data_from_text(text)
            if extracted_data:
                self.cache.set(cache_key, extracted_data)
                self._merge_data(aggregated_data, extracted_data)
                print(f"    - Data extracted and merged from {url}")
            else:
                print(f"    - No data extracted from {url}")
        else:
            print(f"    - Could not fetch text from {url}")

    def aggregate_data(self, website_analysis):
        aggregated_results = {}
        for domain, categories in website_analysis.items():
            print(f"Aggregating data for domain: {domain}")
            aggregated_results[domain] = self._aggregate_domain_data(domain, categories)
        return aggregated_results

    def _aggregate_domain_data(self, domain, categories):
        aggregated_data = self.structured_output_format.copy()
        
        location_complete = False
        pricing_complete = False
        
        for category, urls in categories.items():
            print(f" - Processing category: {category}")
            for url in urls:
                if category == "location_information" and not location_complete:
                    self._process_subpage(url, aggregated_data)
                    if aggregated_data["location_information"]["name"] and aggregated_data["location_information"]["city"]:
                        location_complete = True
                        print(f"   - Location information complete for {domain}")
                elif category == "pricing" and not pricing_complete:
                    self._process_subpage(url, aggregated_data)
                    if (
                        aggregated_data["pricing"]["windsurfing"]["hourly_rate"] and
                        aggregated_data["pricing"]["windsurfing"]["daily_rate"] and
                        aggregated_data["pricing"]["surfing"]["availability"] and
                        aggregated_data["pricing"]["surfing"]["hourly_rate"] and
                        aggregated_data["pricing"]["surfing"]["daily_rate"] and
                        aggregated_data["pricing"]["equipment_rental"]["rental_rate_per_hour"] and
                        aggregated_data["pricing"]["equipment_rental"]["rental_rate_per_day"]
                    ):
                        pricing_complete = True
                        print(f"   - Pricing information complete for {domain}")
                elif category not in ["location_information", "pricing"]:
                    self._process_subpage(url, aggregated_data)
        
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
