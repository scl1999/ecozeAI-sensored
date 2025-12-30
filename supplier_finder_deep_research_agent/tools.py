import os
import requests
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

def browser_use_tool(task: str, urls: Optional[List[str]] = None) -> str:
    """
    Uses a browser agent to navigate websites and extract information.
    Use this tool when you need to interact with a website, find specific information on a page that requires navigation, or when a direct URL analysis is insufficient.

    Args:
        task (str): The specific task for the browser agent to perform (e.g., "Find the supplier name on this page", "Navigate to the about us page and find the address").
        urls (List[str], optional): A list of starting URLs to visit. If not provided, the agent may use search.

    Returns:
        str: The result of the browser task, including any extracted information or errors.
    """
    # URL of the browser-use service (assuming it's running locally or accessible)
    # In a real deployment, this might be an internal DNS name or a Cloud Run URL.
    # For local dev/testing with the provided setup, it seems to be localhost:8080
    # But since this agent might run in Vertex AI Agent Engine, we need to consider connectivity.
    # If running locally via `adk run`, localhost works.
    # If deployed, we might need the public URL of the browser-use service.
    # For now, we'll default to localhost but allow env var override.
    service_url = os.environ.get("BROWSER_USE_SERVICE_URL", "http://localhost:8080/browse")

    try:
        logger.debug(f"browser_use_tool called with task: {task}, urls: {urls}")
        payload = {"task": task}
        if urls:
            payload["urls"] = urls

        logger.info(f"Calling browser-use service at {service_url} with task: {task}")
        response = requests.post(service_url, json=payload, timeout=300) # Long timeout for browser tasks
        response.raise_for_status()
        
        result_json = response.json()
        if result_json.get("success"):
            return result_json.get("result", "No result returned.")
        else:
            return f"Error from browser service: {result_json.get('error', 'Unknown error')}"

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to call browser-use service: {e}")
        return f"Failed to execute browser task: {str(e)}"
