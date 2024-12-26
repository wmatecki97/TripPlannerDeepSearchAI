import json
import os

class Cache:
    def __init__(self, cache_dir="cache"):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, key):
         return os.path.join(self.cache_dir, f"{key}.json")


    def get(self, key):
        cache_path = self._get_cache_path(key)
        if os.path.exists(cache_path):
            with open(cache_path, "r") as f:
                return json.load(f)
        return None

    def set(self, key, value):
        cache_path = self._get_cache_path(key)
        with open(cache_path, "w") as f:
            json.dump(value, f, indent=4)
        return value

if __name__ == '__main__':
    cache = Cache()
    test_key = "test_key"
    test_value = {"test": "value"}
    
    # Test set
    cache.set(test_key, test_value)
    
    # Test get
    retrieved_value = cache.get(test_key)
    print(f"Retrieved value: {retrieved_value}")
    
    # Test get non-existent key
    non_existent_value = cache.get("non_existent_key")
    print(f"Non-existent value: {non_existent_value}")
