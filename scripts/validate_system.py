import requests
import time
import sys
import os

# Add parent directory to path for common imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.logging_utils import get_logger

logger = get_logger("validate_system")

def test_system_connectivity():
    """
    Check if all core microservices are reachable.
    This script assumes the Nexus stack is running via Docker Compose.
    """
    services = {
        "FastAPI (Port 8000)": "http://localhost:8000/health",
        "Prometheus Metrics (Port 9099)": "http://localhost:9099/metrics",
        "Dashboard (Port 8501)": "http://localhost:8501",
        "Ollama API (Port 11434)": "http://localhost:11434/api/tags"
    }

    logger.info("Starting system connectivity check", extra={"service_count": len(services)})
    all_ok = True
    service_results = []
    
    for name, url in services.items():
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code < 400:
                logger.info("Service reachable", extra={
                    "service": name,
                    "url": url,
                    "status_code": resp.status_code
                })
                service_results.append({"service": name, "status": "up", "status_code": resp.status_code})
            else:
                logger.warning("Service responded with error", extra={
                    "service": name,
                    "url": url,
                    "status_code": resp.status_code
                })
                service_results.append({"service": name, "status": "error", "status_code": resp.status_code})
                all_ok = False
        except Exception as e:
            logger.error("Service unreachable", extra={
                "service": name,
                "url": url,
                "error": str(e)
            })
            service_results.append({"service": name, "status": "down", "error": str(e)})
            all_ok = False
    
    if all_ok:
        logger.info("All services reachable successfully", extra={
            "total_services": len(services),
            "healthy_services": len([r for r in service_results if r["status"] == "up"])
        })
        print("\n🎉 All services are reachable!")
    else:
        unhealthy_count = len([r for r in service_results if r["status"] != "up"])
        logger.error("Some services unreachable", extra={
            "total_services": len(services),
            "unhealthy_services": unhealthy_count,
            "results": service_results
        })
        print(f"\n❌ {unhealthy_count} services are unreachable. Ensure 'docker-compose up' is running.")
    
    return all_ok

def test_api_data_flow():
    """Check if the API returns data."""
    logger.info("Starting API data flow check")
    endpoints = [
        "http://localhost:8000/api/v1/kpis",
        "http://localhost:8000/api/kpis",
    ]
    
    try:
        for url in endpoints:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                revenue = data.get('revenue', 0)
                logger.info("API KPI endpoint success", extra={
                    "url": url,
                    "revenue": revenue,
                    "status_code": resp.status_code
                })
                print(f"✅ API KPI Endpoint: SUCCESS ({url})")
                print(f"   Current Revenue: ₹{revenue:,.2f}")
                return
            else:
                logger.warning("API KPI endpoint failed", extra={
                    "url": url,
                    "status_code": resp.status_code
                })
                print(f"❌ API KPI Endpoint: FAILED ({resp.status_code})")
    except Exception as e:
        logger.error("API KPI endpoint unreachable", extra={
            "url": endpoints[0] if endpoints else "unknown",
            "error": str(e)
        })
        print(f"❌ API KPI Endpoint: UNREACHABLE")

if __name__ == "__main__":
    test_system_connectivity()
    test_api_data_flow()
