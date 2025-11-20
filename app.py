from flask import Flask, jsonify
import boto3
from boto3.dynamodb.conditions import Key

app = Flask(__name__)

dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
table = dynamodb.Table('SmartCityEmissions')

@app.route('/api/data')
def get_data():
    # Get latest 50 Bengaluru records (adjust as needed)
    resp = table.query(
        KeyConditionExpression=Key('city_id').eq('Bengaluru'),
        ScanIndexForward=False,    # descending
        Limit=50
    )
    items = resp.get('Items', [])

    # Map data for dashboard (example: aqi, temp, co, etc.)
    dashboard_data = {
        "aqiSeries": [ {"timestamp": it["timestamp"], "aqi": float(it["aqi"])} for it in items ],
        "tempSeries": [ {"timestamp": it["timestamp"], "temp": float(it["temperature_c"])} for it in items ],
        # Add more mappings as your JS dashboard needs
    }
    return jsonify(dashboard_data)

if __name__ == '__main__':
    app.run(debug=True)
