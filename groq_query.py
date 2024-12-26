import os
from groq import Groq
from dotenv import load_dotenv
import json

load_dotenv()

class GroqQuery:
    def __init__(self):
        self.api_key = os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment variables")
        self.client = Groq(api_key=self.api_key)
        self.model = "mixtral-8x7b-32768"

    def query(self, input_text, categories):
        prompt = f"""
        You are an expert in categorizing text.
        Given the input text, determine the probability of it belonging to each of the following categories: {categories}.
        Return a JSON object with the category names as keys and the probabilities as values.
        
        Input text: {input_text}
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
            return chat_completion.choices[0].message.content
        except Exception as e:
            print(f"Error during Groq query: {e}")
            return None

if __name__ == '__main__':
    groq_query = GroqQuery()
    input_text = "This is a test text about windsurfing schools."
    categories = ["windsurf school", "windsurf shop", "article", "other"]
    result = groq_query.query(input_text, categories)
    print(result)
