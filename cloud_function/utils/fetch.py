import os
import requests
from dotenv import load_dotenv

load_dotenv()
BASE_URL = "https://vmpay.vertitecnologia.com.br/api/v1"
ACCESS_TOKEN = os.getenv("VM_API_TOKEN")


def fetch_from_endpoint(endpoint, params=None):
    if params is None:
        params = {}
    params["access_token"] = ACCESS_TOKEN
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
    response.raise_for_status()
    return response.json()
