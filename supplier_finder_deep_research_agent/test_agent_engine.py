import os
from google.auth import default
from google.auth.transport.requests import Request
import requests
import json

# Configuration
PROJECT_ID = "ecoze-f216c"
LOCATION = "europe-west2"
AGENT_ID = "4491601756488204288"

def test_agent_engine():
    print(f"Testing Agent Engine: {AGENT_ID}")
    
    # Get credentials
    credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    credentials.refresh(Request())
    token = credentials.token
    
    url = f"https://{LOCATION}-aiplatform.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION}/reasoningEngines/{AGENT_ID}:streamQuery"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "input": {
            "message": "Find the supplier for material ID: eMpWjd4XUYFOG4kJ5i8j,. Description: A test product description.",
            "user_id": "test-user"
        }
    }
    
    print(f"Sending request to {url}...")
    try:
        response = requests.post(url, headers=headers, json=payload, stream=True)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            print("Success! Streaming response:")
            for line in response.iter_lines():
                if line:
                    print(line.decode('utf-8'))
        else:
            print("Error:")
            print(response.text)
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    test_agent_engine()
