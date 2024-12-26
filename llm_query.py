import os
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

class LLMQuery:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-pro')

    def query(self, input_text, categories):
        prompt = f"""
        You are an expert in categorizing text.
        Given the input text, determine the probability of it belonging to each of the following categories: {categories}.
        Return a JSON object with the category names as keys and the probabilities as values.
        
        Input text: {input_text}
        """
        
        response = self.model.generate_content(prompt)
        try:
            return response.text
        except Exception as e:
            print(f"Error during LLM query: {e}")
            return None

if __name__ == '__main__':
    llm_query = LLMQuery()
    input_text = "This is a test text about windsurfing schools."
    categories = ["windsurf school", "windsurf shop", "article", "other"]
    result = llm_query.query(input_text, categories)
    print(result)
