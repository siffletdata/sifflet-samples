import requests
import yaml
import os
import time
import json


TENANT = "<target tenant>"
TOKEN = "<your token>"

API_URL = f"https://{TENANT}.siffletdata.com/api/v1/rules/_all-as-code"
OUTPUT_DIR = "monitors_as_code"

os.makedirs(OUTPUT_DIR, exist_ok=True)

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/x-yaml"
}

page = 0
page_size = 50
total_monitors = 0
empty_page_received = False

print(f"Starting to download monitors using page size of {page_size}...")

try:
    while not empty_page_received:
        paginated_url = f"{API_URL}?page={page}&size={page_size}&mode=RELAXED"
        
        print(f"Fetching page {page}...")
        
        response = requests.get(paginated_url, headers=headers)
        response.raise_for_status()
        
        print(f"Response status: {response.status_code}")
        print(f"Response content type: {response.headers.get('Content-Type', 'unknown')}")
        
        try:
            monitors_as_code = yaml.safe_load(response.text)
            if monitors_as_code is None:
                monitors_as_code = []
        except yaml.YAMLError:
            try:
                monitors_as_code = json.loads(response.text)
            except json.JSONDecodeError:
                print(f"Warning: Could not parse response as YAML or JSON")
                print(f"Response preview: {response.text[:200]}...")
                monitors_as_code = []
        
        if isinstance(monitors_as_code, str):
            print(f"Warning: Received string instead of list: {monitors_as_code[:200]}...")
            monitors_as_code = []
        
        current_page_count = len(monitors_as_code)
        total_monitors += current_page_count
        
        print(f"Retrieved {current_page_count} monitors from page {page}")
        
        if current_page_count == 0:
            empty_page_received = True
            print("Received empty page, stopping pagination")
            break
        
        for monitor in monitors_as_code:
            monitor_id = monitor['id']
            filename = f"monitor_{monitor_id}.yaml"
            file_path = os.path.join(OUTPUT_DIR, filename)
            
            with open(file_path, "w") as f:
                yaml.dump(monitor, f, sort_keys=False, default_flow_style=False)
        
        page += 1
        time.sleep(0.5)
            
    print(f"Download complete! Saved {total_monitors} monitors to {OUTPUT_DIR}/")
    
except requests.exceptions.RequestException as e:
    print(f"Error fetching monitors: {e}")
    if hasattr(e, 'response') and e.response:
        print(f"Status code: {e.response.status_code}")
        print(f"Response text: {e.response.text}")