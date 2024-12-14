import platform
from inspect import currentframe, getframeinfo
from pathlib import Path
from decouple import config
import requests
import json

DEFAULT_MODEL = "qwen2.5-coder-3b-instruct"
DEFAULT_PORT = 1234

def check_lmstudio_server(port: int = DEFAULT_PORT):
    """Check if LMStudio server is running and the model is loaded."""
    try:
        # Test basic connectivity
        response = requests.get(f"http://localhost:{port}/v1/models")
        if response.status_code != 200:
            return False, "Server not responding correctly"
        
        # Test model availability with a simple completion
        test_request = {
            "model": DEFAULT_MODEL,
            "messages": [{"role": "user", "content": "test"}],
            "max_tokens": 1
        }
        response = requests.post(
            f"http://localhost:{port}/v1/chat/completions",
            headers={"Content-Type": "application/json"},
            json=test_request
        )
        if response.status_code == 200:
            return True, "Server running and model loaded"
        else:
            return False, f"Model {DEFAULT_MODEL} not loaded or not responding"
            
    except requests.RequestException as e:
        return False, f"Connection error: {str(e)}"

def main():
    print(f"Checking LMStudio server for {DEFAULT_MODEL}...")
    
    server_ok, message = check_lmstudio_server()
    if not server_ok:
        print(f"Error: {message}")
        print("\nPlease ensure:")
        print(f"1. LMStudio is running on port {DEFAULT_PORT}")
        print(f"2. {DEFAULT_MODEL} is loaded in LMStudio")
        print("3. The API server is enabled in LMStudio")
        return

    print("\n✅ Success! LMStudio server is running correctly")
    print("\nConfiguration for kotaemon:")
    print(f"  - URL: http://localhost:{DEFAULT_PORT}")
    print("  - API format: OpenAI compatible")
    print(f"  - Model: {DEFAULT_MODEL}")
    print("\nExample curl command to test:")
    print(f"""curl http://localhost:{DEFAULT_PORT}/v1/chat/completions \\
  -H "Content-Type: application/json" \\
  -d '{{
    "model": "{DEFAULT_MODEL}",
    "messages": [{{"role": "user", "content": "Hello!"}}],
    "temperature": 0.7,
    "max_tokens": 100
  }}'""")

if __name__ == "__main__":
    main()