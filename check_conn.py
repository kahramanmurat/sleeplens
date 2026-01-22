import requests
import sys

url = "https://YPPZFHA-ZCC99354.snowflakecomputing.com"
print(f"Testing connection to {url}")
try:
    resp = requests.get(url, timeout=10)
    print(f"Status Code: {resp.status_code}")
except Exception as e:
    print(f"Connection Failed: {e}")
