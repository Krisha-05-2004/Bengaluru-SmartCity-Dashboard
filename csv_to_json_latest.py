import csv
import json
from datetime import datetime
import time

csv_path = r"K:\smartcity\data\bengaluru_timeseries.csv"
out_path = r"K:\smartcity\data\latest.json"

with open(csv_path, newline='', encoding='utf-8') as f:
    reader = list(csv.DictReader(f))
    if not reader:
        raise SystemExit("CSV empty")
    last = reader[-1]

# adapt field names per your CSV columns (aqi, city_id, co, humidity, pm10, pm2_5, rain_1h_mm, temperature_c, timestamp, ttl)
record = {
    "city_id": last.get("city_id") or "Bengaluru",
    "timestamp": last.get("timestamp"),
    "timestamp_utc": last.get("timestamp"),
    "timestamp_ist": last.get("timestamp"),
    "timestamp_epoch": int(float(last.get("ttl", 0))) - (14 * 24 * 3600) if last.get("ttl") else int(time.time()),
    "temperature_c": float(last.get("temperature_c") or 0),
    "humidity": float(last.get("humidity") or 0),
    "wind_speed_m_s": None,
    "rain_1h_mm": float(last.get("rain_1h_mm") or 0),
    "air_raw": {
        "list": [
            {
                "main": {"aqi": int(float(last.get("aqi") or 0))},
                "components": {
                    "co": float(last.get("co") or 0),
                    "pm2_5": float(last.get("pm2_5") or 0),
                    "pm10": float(last.get("pm10") or 0)
                }
            }
        ]
    },
    "co": float(last.get("co") or 0),
    "pm2_5": float(last.get("pm2_5") or 0),
    "pm10": float(last.get("pm10") or 0),
    "aqi": int(float(last.get("aqi") or 0)),
    "ttl": int(float(last.get("ttl") or (time.time() + 14*24*3600)))
}

with open(out_path, 'w', encoding='utf-8') as out:
    json.dump(record, out, ensure_ascii=False, indent=2)

print("Wrote", out_path)
