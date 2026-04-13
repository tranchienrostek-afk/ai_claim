import httpx
import sys
import time
import json

BASE_URL = "http://localhost:9780"

def test_health():
    print("Testing /health...")
    try:
        response = httpx.get(f"{BASE_URL}/health", timeout=10.0)
        print(f"Status: {response.status_code}")
        data = response.json()
        print(json.dumps(data, indent=2))
        if response.status_code != 200:
            return False
        # status can be 'ok' or 'degraded' (if dependencies are down)
        # in CI/CD, 'degraded' might be expected if Neo4j is not in the same docker compose
        return data.get("status") in ["ok", "degraded"]
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

def test_root_summary():
    print("\nTesting /api/knowledge/root-summary...")
    try:
        response = httpx.get(f"{BASE_URL}/api/knowledge/root-summary", timeout=10.0)
        print(f"Status: {response.status_code}")
        if response.status_code != 200:
            return False
        data = response.json()
        print(f"Roots found: {list(data.keys())}")
        return True
    except Exception as e:
        print(f"Root summary failed: {e}")
        return False

def test_upload():
    print("\nTesting /api/knowledge/upload (python-multipart verification)...")
    try:
        files = {"file": ("smoke_test.txt", b"smoke test content", "text/plain")}
        data = {"root_key": "protocols"}
        response = httpx.post(f"{BASE_URL}/api/knowledge/upload", data=data, files=files, timeout=10.0)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            print("Upload successful!")
            return True
        else:
            print(f"Upload failed with body: {response.text}")
            return False
    except Exception as e:
        print(f"Upload failed: {e}")
        return False

if __name__ == "__main__":
    success = True
    if not test_health():
        success = False
    if not test_root_summary():
        success = False
    if not test_upload():
        success = False
    
    if success:
        print("\n✅ All smoke tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Smoke tests failed!")
        sys.exit(1)
