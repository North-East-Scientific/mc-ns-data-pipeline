# api_client.py
"""API client for MasterControl API interactions."""

import requests
import json
import time
import logging
from pandas import json_normalize
from requests.exceptions import HTTPError, ConnectionError, Timeout
from config import API_TOKEN, API_COOKIE, API_ENDPOINTS


class MasterControlAPIClient:
    """Client for interacting with MasterControl API."""
    
    def __init__(self):
        self.headers = {
            'Authorization': f'Bearer {API_TOKEN}',
            'Cookie': API_COOKIE
        }
        self.payload = {}
    
    def perform_get_request(self, url, retries=3, delay=0.2):
        """
        Perform an HTTP GET request with retry logic.
        Returns the response object if successful, otherwise None.
        """
        for retry in range(retries):
            try:
                response = requests.get(url, headers=self.headers, data=self.payload, timeout=10)
                response.raise_for_status()
                return response
            except (HTTPError, ConnectionError, Timeout) as e:
                logging.warning(f"Request failed: {e}. for {url[-18:]} and retry : {retry}")
                time.sleep(delay)
        logging.error(f"Request failed after {retries} retries for URL: {url}")
        return None
    
    def fetch_paginated_data(self, base_url, params=None):
        """
        Fetch all data from a paginated API endpoint.
        Returns a list of all records across all pages.
        """
        all_data = []
        page = 0
        last_page = False
        
        while not last_page:
            url = f"{base_url}?currentPage={page}&itemsPerPage=1000"
            if params:
                for key, value in params.items():
                    url += f"&{key}={value}"
            
            response = self.perform_get_request(url)
            if response is None:
                logging.error(f"Failed to get response from {base_url}")
                break
                
            data = json.loads(response.text)
            
            # Handle different response structures
            if 'content' in data:
                all_data.extend(data.get('content', []))
                last_page = data.get('last', True)
            elif 'pageResult' in data:
                all_data.extend(data.get('pageResult', {}).get('content', []))
                last_page = data.get('last', True)
            else:
                break
            
            page += 1
        
        return all_data
