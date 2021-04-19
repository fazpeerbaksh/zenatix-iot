from flask import Flask, jsonify, request, Response
import random
import csv
import os.path
from os import path

app = Flask(__name__)

FILE_EXISTS = False

def file_exists():
    global FILE_EXISTS

    if FILE_EXISTS:
        return FILE_EXISTS

    FILE_EXISTS = path.exists('sensor_data.csv')
    return FILE_EXISTS

def write_sensor_data(sensor_data):
        try:
            fieldnames = ['Timestamp', 'Value', 'Sensor']
            does_file_exist = file_exists()
            with open('sensor_data.csv', 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not does_file_exist:
                    writer.writeheader()
                writer.writerow(sensor_data)

        except csv.Error as err:
            print("Error occurred while writing {0}".format(err))


@app.route('/live_data', methods=['POST'])
def live_data():
    rnd = random.randint(0, 9)
    data = request.get_json()
    print(data)

    if(rnd < 5): 
        write_sensor_data(data)

        return Response(status=200)

    return Response(status=500)

if __name__ == '__main__':
    app.run(port=5001, host='0.0.0.0', debug=True)