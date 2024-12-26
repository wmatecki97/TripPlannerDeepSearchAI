import os
from groq import Groq
from dotenv import load_dotenv
import json
from cache import Cache

load_dotenv()

class GroqStructuredQuery:
    def __init__(self):
        self.api_key = os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment variables")
        self.client = Groq(api_key=self.api_key)
        self.model = "mixtral-8x7b-32768"
        self.cache = Cache(tool="groq_structured_query")

    def query(self, input_text, structured_output_format):
        cache_key = f"{input_text}_{json.dumps(structured_output_format)}"
        cached_result = self.cache.get(cache_key)
        if cached_result:
            print("  - Returning cached result")
            return cached_result
        
        prompt = f"""You are an expert in extracting information from text.
        Given the input text, extract the information and return a JSON object using the following format:
        {json.dumps(structured_output_format)}
        Text: {input_text}
        """
        try:
            chat_completion = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": prompt,
                    }
                ],
                model=self.model,
                response_format={"type": "json_object"}
            )
            result = json.loads(chat_completion.choices[0].message.content)
            self.cache.set(cache_key, result)
            return result
        except Exception as e:
            print(f"Error during Groq query: {e}")
            return None

if __name__ == '__main__':
    groq_query = GroqStructuredQuery()
    input_text = "This is a test text about a windsurfing school. They offer hourly rentals for 20 euros and daily rentals for 50 euros."
    structured_output_format = {
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
    result = groq_query.query(input_text, structured_output_format)
    print(json.dumps(result, indent=4))
    
    # Test cache
    result = groq_query.query(input_text, structured_output_format)
    print(json.dumps(result, indent=4))
