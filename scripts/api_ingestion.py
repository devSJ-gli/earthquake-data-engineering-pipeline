import json
import requests


API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

RAW_OUTPUT_FILE = "earthquakes_raw.json"
L1_OUTPUT_FILE = "earthquakes_l1.jsonl"


def fetch_earthquakes(limit=100):
    """Fetch earthquake data from the USGS Earthquake API."""

    params = {
        "format": "geojson",
        "limit": limit,
        "orderby": "time",
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def save_raw_response(data):
    """Save the original USGS API response as JSON."""

    with open(RAW_OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)


def create_l1_jsonl(data):
    """Normalize USGS GeoJSON features into L1 JSONL records."""

    with open(L1_OUTPUT_FILE, "w", encoding="utf-8") as file:

        for feature in data.get("features", []):

            properties = feature.get("properties", {})
            coordinates = feature.get("geometry", {}).get("coordinates", [])

            record = {
                "earthquake_id": feature.get("id"),
                "magnitude": properties.get("mag"),
                "place": properties.get("place"),
                "event_time_ms": properties.get("time"),
                "updated_time_ms": properties.get("updated"),
                "status": properties.get("status"),
                "event_type": properties.get("type"),
                "magnitude_type": properties.get("magType"),
                "tsunami": properties.get("tsunami"),
                "significance": properties.get("sig"),
                "longitude": coordinates[0] if len(coordinates) > 0 else None,
                "latitude": coordinates[1] if len(coordinates) > 1 else None,
                "depth_km": coordinates[2] if len(coordinates) > 2 else None,
            }

            file.write(json.dumps(record) + "\n")


def main():
    """Run the USGS extraction and L1 normalization process."""

    print("Fetching earthquake data...")

    data = fetch_earthquakes(limit=100)

    records = data.get("features", [])

    print("API request successful")
    print(f"Records received: {len(records)}")

    save_raw_response(data)
    create_l1_jsonl(data)

    print(f"Raw response saved to: {RAW_OUTPUT_FILE}")
    print(f"L1 JSONL created: {L1_OUTPUT_FILE}")


if __name__ == "__main__":
    main()