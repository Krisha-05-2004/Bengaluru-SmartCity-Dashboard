import os
import json
import boto3
from botocore.exceptions import ClientError

_secrets_client = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION", "ap-south-1"))
_cached_secrets = {}

def get_secret(secret_name):
    """Return parsed JSON secret or raw string value. Caches result in-memory."""
    if secret_name in _cached_secrets:
        return _cached_secrets[secret_name]
    try:
        resp = _secrets_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # handle/log accordingly
        raise
    secret_string = resp.get("SecretString")
    try:
        parsed = json.loads(secret_string)
    except Exception:
        parsed = secret_string
    _cached_secrets[secret_name] = parsed
    return parsed

# Usage example in handler:
def lambda_handler(event, context):
    secret = get_secret("smartcity/openweather")  # name of the secret in Secrets Manager
    if isinstance(secret, dict):
        openweather_key = secret.get("OPENWEATHER_API_KEY")
    else:
        openweather_key = secret  # if stored as plain string
    # continue using openweather_key...
