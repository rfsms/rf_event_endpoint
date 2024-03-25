from flask import Flask, request, jsonify, abort
import mysql.connector
from dotenv import load_dotenv
import os
from datetime import datetime
import json
import logging


app = Flask(__name__)

# Load environment variables from .env file
load_dotenv()


# MySQL database configuration
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
}

# Establish a connection to the database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

files_directory = 'files'
if not os.path.exists(files_directory):
    os.makedirs(files_directory)

logging.basicConfig(level=logging.INFO)
app.logger.addHandler(logging.StreamHandler())


@app.route('/api/data', methods=['POST'])
def post_data():
    try:
        # Get the JSON data from the request
        json_data = request.json

        app.logger.info(f'Received data')

        # Check if JSON data is empty or missing required fields
        if not json_data or not json_data.get('events'):
            app.logger.error(f'No data found in request')
            return jsonify({'error': 'No data found in request'}), 400

        # Extract values from the JSON data
        values = (
            json_data['events'][0]['PCI'],
            json_data['events'][0]['_id'],
            json_data['events'][0]['beam'],
            json_data['events'][0]['carrierID'],
            json_data['events'][0]['cellID'],
            json_data['events'][0]['eNodeB'],
            json_data['events'][0]['elevationAngle'],
            json_data['events'][0]['elevationAngleUnits'],
            json_data['events'][0]['eventID'],
            json_data['events'][0]['headingAzimuth'],
            json_data['events'][0]['headingAzimuthUnits'],
            json_data['events'][0]['inverseAxialRatio'],
            '['+'"'+json_data['events'][0]['labels'][0]+'"'+']',
            json_data['events'][0]['locationLat'],
            json_data['events'][0]['locationLatUnits'],
            json_data['events'][0]['locationLon'],
            json_data['events'][0]['locationLonUnits'],
            json_data['events'][0]['maxBandwidth'],
            json_data['events'][0]['maxBandwidthUnits'],
            json_data['events'][0]['maxFrequency'],
            json_data['events'][0]['maxFrequencyUnits'],
            json_data['events'][0]['maxPower'],
            json_data['events'][0]['maxPowerUnits'],
            json_data['events'][0]['mode'],
            json_data['events'][0]['notifyCarrier'],
            json_data['events'][0]['remoteID'],
            json_data['events'][0]['severityLevel'],
            json_data['events'][0]['signalType'],
            json_data['events'][0]['tiltAngle'],
            json_data['events'][0]['tiltAngleUnits'],
            json_data['events'][0]['timestamp']
        )

        current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_name = f"{current_time}.json"
        file_path = os.path.join(files_directory, file_name)

        with open(file_path, 'w') as file:
            json.dump(json_data, file)

        sql_query = """INSERT INTO rf_events (
                        PCI, _id, beam, carrierID, cellID, eNodeB,
                        elevationAngle, elevationAngleUnits, eventID,
                        headingAzimuth, headingAzimuthUnits, inverseAxialRatio,
                        labels, locationLat, locationLatUnits, locationLon,
                        locationLonUnits, maxBandwidth, maxBandwidthUnits,
                        maxFrequency, maxFrequencyUnits, maxPower, maxPowerUnits,
                        mode, notifyCarrier, remoteID, severityLevel,
                        signalType, tiltAngle, tiltAngleUnits, timestamp
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Execute the SQL query
        cursor.execute(sql_query, values)

        # Commit the changes to the database
        conn.commit()

        # Log success message
        app.logger.info('Data is saved successfully')

        return jsonify({'message': 'Data inserted successfully'}), 201

    except Exception as e:
        # Log error message
        app.logger.error(f'Data insertion failed, Missing Data: {str(e)}')

        return jsonify({'error': f'Data insertion failed, Missing Data: {str(e)}'}), 500


if __name__ == '__main__':
    # Set the port for running on localhost
    port = int(os.getenv('PORT', 4000))
    app.run(port=port)
