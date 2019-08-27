from flask import Flask, render_template, request
from pymongo import MongoClient
from bson import json_util
import setup

# Set up Flask and Mongo
app = Flask(__name__)
client = MongoClient()

@app.route("/on_time_performance")
def on_time_performance():
    carrier = request.args.get('Carrier')
    flight_date = request.args.get('FlightDate')
    flight_num = request.args.get('FlightNum')
    flight = client.agile_data_science.on_time_performance.find_one({
        'Carrier': carrier,
        'FlightDate': flight_date,
        'FlightNum': flight_num
    })
    
    return render_template('flight.html', flight=flight)

# Controller: Fetch all flights between cities on a given day and display them
@app.route("/flights/<origin>/<dest>/<flight_date>")
def list_flights(origin, dest, flight_date):
    
    flights = client.agile_data_science.on_time_performance.find({
        'Origin': origin, 'Dest': dest, 'FlightDate': flight_date},
        sort = [('DepTime', 1),('ArrTime', 1)])
    
    flight_count = flights.count()
    return render_template('flights.html', flights=flights, flight_date=flight_date, flight_count=flight_count)

# Fetch all flights made by a particular airplane, identified by its tail number
@app.route("/airplane/flights/<tail_number>")
def flights_per_airplane(tail_number):
    flights = client.agile_data_science.flights_per_airplane.find_one({'TailNum': tail_number})
    return render_template('flights_per_airplane.html', flights=flights, tail_number=tail_number)

@app.route("/airline/<carrier_code>")
def airline(carrier_code):
    airline_airplanes = client.agile_data_science.airplanes_per_carrier.find({'Carrier': carrier_code})
    return render_template('airlines.html',airline_airplanes=airline_airplanes,carrier_code=carrier_code)

if __name__ == "__main__":
    app.run(debug=True)
