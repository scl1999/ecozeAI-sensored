import google.auth
from google.auth.transport.requests import Request
import requests
import json

PROJECT_ID = "ecoze-f216c"
LOCATION = "europe-west2"
ACTIVE_AGENT_ID = "8726111306123313152"

def cleanup_agents():
    credentials, project = google.auth.default()
    credentials.refresh(Request())
    token = credentials.token
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # List agents
    list_url = f"https://{LOCATION}-aiplatform.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION}/reasoningEngines"
    print(f"Listing agents from: {list_url}")
    
    response = requests.get(list_url, headers=headers)
    if response.status_code != 200:
        print(f"Error listing agents: {response.status_code} {response.text}")
        return

    data = response.json()
    reasoning_engines = data.get("reasoningEngines", [])
    
    print(f"Found {len(reasoning_engines)} agents.")
    
    for engine in reasoning_engines:
        name = engine.get("name") # format: projects/.../locations/.../reasoningEngines/{ID}
        agent_id = name.split("/")[-1]
        
        if agent_id == ACTIVE_AGENT_ID:
            print(f"Skipping active agent: {agent_id}")
            # continue  # User requested to delete all previous versions, so we disable the skip
            pass
            
        print(f"Deleting agent: {agent_id} ({name})")
        delete_url = f"https://{LOCATION}-aiplatform.googleapis.com/v1/{name}?force=true"
        del_response = requests.delete(delete_url, headers=headers)
        
        if del_response.status_code == 200:
            print(f"Successfully deleted {agent_id}")
            # Wait for operation? Deletion returns an LRO (Long Running Operation)
            # We can just fire and forget for now, or print the LRO name
            print(f"Deletion LRO: {del_response.json().get('name')}")
        else:
            print(f"Failed to delete {agent_id}: {del_response.status_code} {del_response.text}")

if __name__ == "__main__":
    cleanup_agents()
