# Import the dependencies.
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
import datetime as dt
import pandas as pd

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route('/')
def home():
    return (
        f"Welcome to the Climate App!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end"
    )

# Define the precipitation route
@app.route('/api/v1.0/precipitation')
def precipitation():
    # Query the last 12 months of precipitation data
    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    one_year_ago = pd.to_datetime(latest_date.date) - pd.DateOffset(years=1)
    precipitation_data = session.query(Measurement.date, Measurement.prcp).\
    filter(func.date(Measurement.date) >= one_year_ago.date()).\
    order_by(Measurement.date).all()

    # Convert the query results to a dictionary
    prcp_dict = {date: prcp for date, prcp in precipitation_data}

    return jsonify(prcp_dict)

# Define the stations route
@app.route('/api/v1.0/stations')
def stations():
    # Query all stations
    all_stations = session.query(Station.station).all()

    # Convert the query results to a list
    station_list = [station for station, in all_stations]

    return jsonify(station_list)

# Define the tobs route
@app.route('/api/v1.0/tobs')
def tobs():
    # Find the most active station
    most_active_station_id = session.query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).first()[0]
    
    # Query the dates and temperature observations of the most-active station for the previous year of data
    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    one_year_ago = pd.to_datetime(latest_date.date) - pd.DateOffset(years=1)
    most_active_station_tobs = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.station == most_active_station_id).\
        filter(Measurement.date >= one_year_ago.strftime('%Y-%m-%d')).all()

    # Convert the query results to a list of dictionaries
    tobs_list = [{'Date': date, 'Temperature': tobs} for date, tobs in most_active_station_tobs]

    return jsonify(tobs_list)

# Define the start and start/end route
@app.route('/api/v1.0/<start>')
@app.route('/api/v1.0/<start>/<end>')
def temperature_stats(start, end=None):
    # Convert start and end dates to datetime objects
    start_date = dt.datetime.strptime(start, '%Y-%m-%d')
    end_date = dt.datetime.strptime(end, '%Y-%m-%d') if end else None

    # Query temperature statistics based on the specified start and end dates
    if end_date:
        temperature_stats = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    else:
        temperature_stats = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).all()

    # Convert the query results to a dictionary
    stats_dict = {'TMIN': temperature_stats[0][0], 'TAVG': temperature_stats[0][1], 'TMAX': temperature_stats[0][2]}

    return jsonify(stats_dict)

# Run the app
if __name__ == '__main__':
    app.run(debug=True)