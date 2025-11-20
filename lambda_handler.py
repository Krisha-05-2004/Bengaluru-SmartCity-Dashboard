# lambda_handler.py
import boto3, csv, io, json, os

s3 = boto3.client('s3')
BUCKET = os.environ.get('BUCKET_NAME')  # set in Lambda env
RAW_KEY = os.environ.get('RAW_KEY', 'raw/bengaluru_timeseries.csv')
OUT_KEY = os.environ.get('OUT_KEY', 'data/latest.json')

def safe_float(x):
    try:
        return float(x) if x!='' else None
    except:
        return None

def normalize_row(r):
    # choose available keys
    ts = r.get('timestamp_c') or r.get('timestamp') or r.get('ts') or ''
    return {
        "timestamp_c": ts,
        "aqi": safe_float(r.get('aqi') or r.get('AQI')),
        "temperature_c": safe_float(r.get('temperature_c') or r.get('temp')),
        "pm2_5": safe_float(r.get('pm2_5') or r.get('pm25')),
        "humidity": safe_float(r.get('humidity')),
        "rain_1h": safe_float(r.get('rain_1h') or r.get('rain')),
        "city_id": r.get('city_id') or r.get('city') or 'Unknown'
    }

def lambda_handler(event, context):
    try:
        res = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)
        body = res['Body'].read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(body))
        rows = [normalize_row(r) for r in reader if (r.get('timestamp_c') or r.get('timestamp') or r.get('ts'))]
        # optional: sort
        rows = sorted(rows, key=lambda x: x.get('timestamp_c') or '')
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(rows, indent=2).encode('utf-8'), ContentType='application/json')
        return {"status": "ok", "written": len(rows)}
    except Exception as e:
        print("ERR", e)
        raise
