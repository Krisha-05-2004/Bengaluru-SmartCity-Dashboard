# app.py (snippet to replace or integrate near the top of the file)

import os
import json
import time
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# ---------- configuration ----------
SECRET_NAME = os.getenv("OPENWEATHER_SECRET_NAME", "smartcity/openweather")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
# -----------------------------------

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Cached clients / resources to reduce init cost across invocations
_boto_session = boto3.session.Session(region_name=AWS_REGION)
_secrets_client = _boto_session.client("secretsmanager")
_dynamodb = _boto_session.resource("dynamodb")
_s3 = _boto_session.client("s3")

# Replace with your actual table name or env var
DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME", "SmartCityEmissions")
RAW_BUCKET = os.getenv("RAW_BUCKET", "smartcity-raw-075540751057")
PROC_BUCKET = os.getenv("PROC_BUCKET", "smartcity-processed-075540751057")

_table = _dynamodb.Table(DDB_TABLE_NAME)

# Simple in-memory cache for secrets
_secret_cache = {}

def get_secret(secret_name=SECRET_NAME):
    """Fetch secret value from Secrets Manager and cache it in-memory."""
    if secret_name in _secret_cache:
        return _secret_cache[secret_name]

    try:
        resp = _secrets_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.exception("Failed to read secret %s", secret_name)
        raise

    secret_string = resp.get("SecretString")
    try:
        parsed = json.loads(secret_string)
    except Exception:
        parsed = secret_string
    _secret_cache[secret_name] = parsed
    return parsed

# requests fallback wrapper
_requests_session = None
def fetch_json(url, params=None, timeout=10):
    """Try requests (preferred). If not available, fall back to urllib."""
    global _requests_session
    try:
        if _requests_session is None:
            import requests
            _requests_session = requests.Session()
        r = _requests_session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        # fallback using urllib for simple GETs
        try:
            import urllib.request
            from urllib.parse import urlencode
            if params:
                url = url + "?" + urlencode(params)
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.load(resp)
        except Exception as e2:
            logger.exception("Both requests and urllib failed for %s", url)
            raise

# Example processing function — integrate your existing logic here
def process_and_store(weather_payload):
    ts = datetime.now(timezone.utc)
    item = {
        "timestamp": ts.isoformat(),
        "payload": weather_payload
    }
    # Write to DynamoDB
    _table.put_item(Item=item)
    # Save raw + processed to S3 (keys / names adapt to your format)
    raw_key = f"raw/Bengaluru/{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
    proc_key = f"processed/Bengaluru/{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
    _s3.put_object(Bucket=RAW_BUCKET, Key=raw_key, Body=json.dumps(weather_payload).encode("utf-8"))
    _s3.put_object(Bucket=PROC_BUCKET, Key=proc_key, Body=json.dumps({'status':'ok','ts':ts.isoformat()}).encode("utf-8"))
    logger.info("Saved raw JSON to s3://%s/%s", RAW_BUCKET, raw_key)
    logger.info("Saved processed JSON to s3://%s/%s", PROC_BUCKET, proc_key)

# Lambda handler
def lambda_handler(event, context):
    # Get API key from Secrets Manager
    secret = get_secret(SECRET_NAME)
    if isinstance(secret, dict):
        openweather_key = secret.get("OPENWEATHER_API_KEY")
    else:
        openweather_key = secret

    if not openweather_key:
        logger.error("OpenWeather API key not found in secret %s", SECRET_NAME)
        return {"statusCode": 500, "body": json.dumps({"error": "missing_api_key"})}

    # Build OpenWeather API request (example: current weather)
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"lat": os.getenv("LAT", "12.9716"), "lon": os.getenv("LON", "77.5946"), "appid": openweather_key, "units": "metric"}

    # Fetch, process, and store
    try:
        payload = fetch_json(url, params=params, timeout=10)
        process_and_store(payload)
    except Exception as e:
        logger.exception("Failed to fetch/process weather")
        return {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}

    # Return friendly response (keep same format you used earlier)
    now_utc = datetime.now(timezone.utc)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "status": "ok",
            "timestamp_utc": now_utc.isoformat(),
            "timestamp_ist": now_utc.astimezone(timezone.utc).isoformat()
        })
    }
