import logging
import azure.functions as func
from azure.functions import TimerRequest
from datetime import datetime, timezone
from datetime import timedelta
import json
import httpx
from typing import Dict, Any, List, Optional


def main(mytimer: TimerRequest, outputBlob: func.Out[str]) -> None:
    utc_timestamp = datetime.now(timezone.utc).replace(tzinfo=timezone.utc).isoformat()
# datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')

    # BOX_ID = "6a1564a7253e80000738f37c" # hanoi not update frequent
    BOX_ID = "69b57b2f433eff00073cc7d9" # thailand 
    BASE_URL = "https://api.opensensemap.org"

    to_date = datetime.now(timezone.utc).replace( minute = 0, second=0, microsecond=0)
    from_date = to_date - timedelta(hours=1)

    from_date_str = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_date_str = to_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"Fetching data from {from_date_str} to {to_date_str}")
    try:
        with httpx.Client(timeout=30.0) as client:
            # 2. Get box metadata to discover sensor IDs
            box_resp = client.get(f"{BASE_URL}/boxes/{BOX_ID}?format=json")
            box_resp.raise_for_status()
            box_data = box_resp.json()

            sensors = box_data.get("sensors", [])
            box_measurements = {}

            box_measurements["device_id"] = BOX_ID
            box_measurements["device_name"] = box_data.get("name")
            box_measurements["model"] = box_data.get("model")
            box_measurements["currentLocation"] = box_data.get("currentLocation")

            # 3. Fetch last 10 minutes for each sensor
            for sensor in sensors:
                sensor_id = sensor["_id"]
                title = sensor.get("title")
                unit = sensor.get("unit")
                sensor_type = sensor.get("sensorType")

                # Endpoint format:
                # https://api.opensensemap.org/boxes/:senseBoxId/data/:sensorId?from-date=fromDate&to-date=toDate&download=true&format=json
                data_url = f"{BASE_URL}/boxes/{BOX_ID}/data/{sensor_id}"
                params = {
                    "from-date": from_date_str,
                    "to-date": to_date_str,
                    "download": "true",
                    "format": "json",
                }

                resp = client.get(data_url, params=params)
                resp.raise_for_status()
                measurements = resp.json()

                box_measurements[title] = {
                    "metric_name": title,
                    "ingest_time": to_date_str,
                    "sensor_id": sensor_id,
                    "sensor_type": sensor_type,
                    "unit": unit,
                    "data": measurements,
                }
                    # print(f" - {title} ({sensor_id}): {len(measurements)} measurements found")

                # return box_measurements
        outputBlob.set(json.dumps(box_measurements))
    except Exception as e:
        logging.error(f"Failed to fetch: {e}")
        raise e

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    