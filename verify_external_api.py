import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("ELECTRICITYMAPS_API_TOKEN")
ZONE = "US-CAL-CISO"


def check_external_api():
    print(f"Checking ElectricityMaps API for zone {ZONE}...")
    if not TOKEN:
        print("❌ ELECTRICITYMAPS_API_TOKEN not found in environment.")
        return

    url = "https://api-access.electricitymaps.com/free-tier/carbon-intensity/latest"
    headers = {"auth-token": TOKEN}
    params = {"zone": ZONE}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("✅ External API Check Passed!")
            print(f"   Carbon Intensity: {data.get('carbonIntensity')} gCO2/kWh")
            print(f"   Timestamp: {data.get('datetime')}")
        else:
            print(f"❌ External API Check Failed: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"❌ External API Check Error: {e}")


if __name__ == "__main__":
    check_external_api()
