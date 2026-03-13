import requests
import time
import sys

def test_system_connectivity():
    """
    Check if all core microservices are reachable.
    This script assumes the Nexus stack is running via Docker Compose.
    """
    services = {
        "FastAPI (Port 8000)": "http://localhost:8000/health",
        "Prometheus Metrics (Port 9090)": "http://localhost:9090/metrics",
        "Dashboard (Port 8501)": "http://localhost:8501",
        "Ollama API (Port 11434)": "http://localhost:11434/api/tags"
    }

    print("--- Nexus Integration Connectivity Check ---")
    all_ok = True
    for name, url in services.items():
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code < 400:
                print(f"✅ {name}: UP")
            else:
                print(f"⚠️ {name}: RESPONDED WITH {resp.status_code}")
                all_ok = False
        except Exception as e:
            print(f"❌ {name}: DOWN ({str(e)})")
            all_ok = False
    
    if all_ok:
        print("\n🎉 All services are reachable!")
    else:
        print("\n❌ Some services are unreachable. Ensure 'docker-compose up' is running.")
    
    return all_ok

def test_api_data_flow():
    """Check if the API returns data."""
    print("\n--- Nexus API Data Flow Check ---")
    try:
        resp = requests.get("http://localhost:8000/api/kpis", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            print(f"✅ API KPI Endpoint: SUCCESS")
            print(f"   Current Revenue: ₹{data.get('revenue', 0):,.2f}")
        else:
            print(f"❌ API KPI Endpoint: FAILED ({resp.status_code})")
    except Exception as e:
        print(f"❌ API KPI Endpoint: UNREACHABLE")

if __name__ == "__main__":
    test_system_connectivity()
    test_api_data_flow()
