# fetch_city_data/app.py
import os
import json
import time
import logging
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
import requests
from requests.adapters import HTTPAdapter, Retry

# Setup logging
logger = logging.getLogger("fetch_city_data")
logger.setLevel(logging.INFO)

# AWS clients / resources
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

# Environment config (set these in Lambda env vars)
TABLE = os.environ.get("DDB_TABLE", "SmartCityEmissions")
RAW_BUCKET = (os.environ.get("RAW_BUCKET") or "").strip() or None
PROC_BUCKET = (os.environ.get("PROC_BUCKET") or os.environ.get("PROCESSED_BUCKET") or "").strip() or None
CITY = os.environ.get("CITY_NAME", "Bengaluru")
OW_KEY = os.environ.get("OPENWEATHER_API_KEY")

# Coordinates
LAT = float(os.environ.get("LAT", 12.9716))
LON = float(os.environ.get("LON", 77.5946))

table = dynamodb.Table(TABLE)

# Requests session with retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST"])
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))


def convert_numbers(obj):
    """
    Recursively:
      - convert int/float -> Decimal (for boto3/dynamodb)
      - remove keys with value None
    """
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            converted = convert_numbers(v)
            if converted is not None:
                out[k] = converted
        return out
    if isinstance(obj, list):
        out = []
        for v in obj:
            converted = convert_numbers(v)
            if converted is not None:
                out.append(converted)
        return out
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, (int, float)) and not isinstance(obj, bool):
        # Use str() to preserve numeric representation
        return Decimal(str(obj))
    if obj is None:
        return None
    return obj


def fetch_weather(lat=LAT, lon=LON, key=OW_KEY):
    if not key:
        raise RuntimeError("OPENWEATHER_API_KEY not set in environment")
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}&units=metric"
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    # parse JSON from text if you want Decimal conversion here: json.loads(resp.text, parse_float=Decimal)
    return resp.json()


def fetch_air(lat=LAT, lon=LON, key=OW_KEY):
    if not key:
        raise RuntimeError("OPENWEATHER_API_KEY not set in environment")
    # Use https
    url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}"
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def safe_s3_key(prefix, city, dt):
    """
    Create safe S3 object key, e.g. raw/Bengaluru/20251119T060056Z.json
    """
    ts = dt.strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}/{city}/{ts}.json"


def lambda_handler(event, context):
    now_utc = datetime.now(timezone.utc)
    timestamp_utc = now_utc.isoformat()
    timestamp_epoch = int(time.time())
    ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    timestamp_ist = ist.isoformat()

    logger.info("Invocation start: %s (epoch=%s)", timestamp_utc, timestamp_epoch)

    # Fetch
    try:
        weather = fetch_weather()
        air = fetch_air()
    except Exception as e:
        logger.exception("Fetch error")
        raise

    # Extract a few fields safely
    try:
        comp = air.get("list", [{}])[0].get("components", {})
        main = air.get("list", [{}])[0].get("main", {})
        co = comp.get("co")
        pm2_5 = comp.get("pm2_5")
        pm10 = comp.get("pm10")
        aqi = main.get("aqi")
    except Exception:
        co = pm2_5 = pm10 = aqi = None

    # Construct record (record will contain native floats; we convert before DB write)
    record = {
        "city_id": CITY,
        "timestamp": timestamp_ist,         # primary key (string) — using IST now
        "timestamp_utc": timestamp_utc,
        "timestamp_ist": timestamp_ist,
        "timestamp_epoch": timestamp_epoch,
        "temperature_c": (weather.get("main", {}) or {}).get("temp"),
        "humidity": (weather.get("main", {}) or {}).get("humidity"),
        "wind_speed_m_s": (weather.get("wind", {}) or {}).get("speed"),
        "rain_1h_mm": ((weather.get("rain") or {}).get("1h", 0) or 0),
        "air_raw": air,
        "co": co,
        "pm2_5": pm2_5,
        "pm10": pm10,
        "aqi": aqi,
        # TTL 14 days from now (in seconds since epoch)
        "ttl": int(time.time()) + 60 * 60 * 24 * 14
    }

    # Convert numbers -> Decimal and drop None for DynamoDB
    clean_record = convert_numbers(record)

    # Write to DynamoDB
    try:
        table.put_item(Item=clean_record)
        logger.info("Wrote item to DynamoDB table %s (key=%s)", TABLE, record["timestamp"])
    # record["timestamp"] is now the IST string
    except Exception as e:
        logger.exception("DynamoDB put_item error")
        raise

    # Save raw JSON to S3 (original JSON, keeps native numeric types)
    if RAW_BUCKET:
        try:
            raw_key = safe_s3_key("raw", CITY, now_utc)
            s3.put_object(Bucket=RAW_BUCKET, Key=raw_key,
                          Body=json.dumps({"weather": weather, "air": air}, separators=(",", ":"), ensure_ascii=False),
                          ContentType="application/json")
            logger.info("Saved raw JSON to s3://%s/%s", RAW_BUCKET, raw_key)
        except Exception:
            logger.exception("S3 put_object (raw) error")
            raise
    else:
        logger.info("RAW_BUCKET not configured; skipping raw S3 upload")

    # Save processed JSON to S3 (original record - not Decimal-converted)
    if PROC_BUCKET:
        try:
            proc_key = safe_s3_key("processed", CITY, now_utc)
            s3.put_object(Bucket=PROC_BUCKET, Key=proc_key,
                          Body=json.dumps(record, default=str, separators=(",", ":"), ensure_ascii=False),
                          ContentType="application/json")
            logger.info("Saved processed JSON to s3://%s/%s", PROC_BUCKET, proc_key)
        except Exception:
            logger.exception("S3 put_object (processed) error")
            raise
    else:
        logger.info("PROC_BUCKET not configured; skipping processed S3 upload")

    response = {"status": "ok", "timestamp_utc": timestamp_utc, "timestamp_ist": timestamp_ist, "timestamp_epoch": timestamp_epoch}
    return {"statusCode": 200, "body": json.dumps(response)}
