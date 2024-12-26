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
            if extracted_
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
        # Merge location information
        if new_data.get("location_information"):
            if not aggregated_data["location_information"]["name"]:
                aggregated_data["location_information"]["name"] = new_data["location_information"].get("name")
            if not aggregated_data["location_information"]["city"]:
                aggregated_data["location_information"]["city"] = new_data["location_information"].get("city")
            if not aggregated_data["location_information"]["contact_details"]["phone"]:
                aggregated_data["location_information"]["contact_details"]["phone"] = new_data["location_information"]["contact_details"].get("phone")
            if not aggregated_data["location_information"]["contact_details"]["email"]:
                aggregated_data["location_information"]["contact_details"]["email"] = new_data["location_information"]["contact_details"].get("email")
            if not aggregated_data["location_information"]["comments"]:
                aggregated_data["location_information"]["comments"] = new_data["location_information"].get("comments")

        # Merge pricing information
        if new_data.get("pricing"):
            if not aggregated_data["pricing"]["windsurfing"]["hourly_rate"]:
                aggregated_data["pricing"]["windsurfing"]["hourly_rate"] = new_data["pricing"]["windsurfing"].get("hourly_rate")
            if not aggregated_data["pricing"]["windsurfing"]["daily_rate"]:
                aggregated_data["pricing"]["windsurfing"]["daily_rate"] = new_data["pricing"]["windsurfing"].get("daily_rate")
            if not aggregated_data["pricing"]["windsurfing"]["package_3_to_7_days"]:
                aggregated_data["pricing"]["windsurfing"]["package_3_to_7_days"] = new_data["pricing"]["windsurfing"].get("package_3_to_7_days")
            if not aggregated_data["pricing"]["surfing"]["availability"]:
                aggregated_data["pricing"]["surfing"]["availability"] = new_data["pricing"]["surfing"].get("availability")
            if not aggregated_data["pricing"]["surfing"]["hourly_rate"]:
                aggregated_data["pricing"]["surfing"]["hourly_rate"] = new_data["pricing"]["surfing"].get("hourly_rate")
            if not aggregated_data["pricing"]["surfing"]["daily_rate"]:
                aggregated_data["pricing"]["surfing"]["daily_rate"] = new_data["pricing"]["surfing"].get("daily_rate")
            if not aggregated_data["pricing"]["equipment_rental"]["included_in_pricing"]:
                aggregated_data["pricing"]["equipment_rental"]["included_in_pricing"] = new_data["pricing"]["equipment_rental"].get("included_in_pricing")
            if not aggregated_data["pricing"]["equipment_rental"]["rental_rate_per_hour"]:
                aggregated_data["pricing"]["equipment_rental"]["rental_rate_per_hour"] = new_data["pricing"]["equipment_rental"].get("rental_rate_per_hour")
            if not aggregated_data["pricing"]["equipment_rental"]["rental_rate_per_day"]:
                aggregated_data["pricing"]["equipment_rental"]["rental_rate_per_day"] = new_data["pricing"]["equipment_rental"].get("rental_rate_per_day")
            if not aggregated_data["pricing"]["equipment_insurance"]["included"]:
                aggregated_data["pricing"]["equipment_insurance"]["included"] = new_data["pricing"]["equipment_insurance"].get("included")
            if not aggregated_data["pricing"]["equipment_insurance"]["cost_per_day"]:
                aggregated_data["pricing"]["equipment_insurance"]["cost_per_day"] = new_data["pricing"]["equipment_insurance"].get("cost_per_day")
            if not aggregated_data["pricing"]["comments"]:
                aggregated_data["pricing"]["comments"] = new_data["pricing"].get("comments")

        # Merge courses information
        if new_data.get("courses"):
            for course in new_data["courses"]:
                aggregated_data["courses"].append(course)

        # Merge transport options
        if new_data.get("transport_options"):
            if not aggregated_data["transport_options"]["pickup_service"]["availability"]:
                aggregated_data["transport_options"]["pickup_service"]["availability"] = new_data["transport_options"]["pickup_service"].get("availability")
            if not aggregated_data["transport_options"]["pickup_service"]["cost"]:
                aggregated_data["transport_options"]["pickup_service"]["cost"] = new_data["transport_options"]["pickup_service"].get("cost")
            if not aggregated_data["transport_options"]["pickup_service"]["from_airport"]:
                aggregated_data["transport_options"]["pickup_service"]["from_airport"] = new_data["transport_options"]["pickup_service"].get("from_airport")
            if not aggregated_data["transport_options"]["pickup_service"]["from_city_center"]:
                aggregated_data["transport_options"]["pickup_service"]["from_city_center"] = new_data["transport_options"]["pickup_service"].get("from_city_center")

if __name__ == '__main__':
    analyzer = WindsurfWebsiteAnalyzer()
    area = "Lanzarote"
    website_analysis = analyzer.analyze_websites(area)
    
    aggregator = WindsurfDataAggregator()
    aggregated_data = aggregator.aggregate_data(website_analysis)
    print(json.dumps(aggregated_data, indent=4))
