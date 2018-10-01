# -*- coding: utf-8 -*-


import csv
from datetime import datetime
import os

import pendulum
from sqlalchemy import create_engine
from sqlalchemy import Column, DateTime, Integer, String

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class Ride(Base):
    """ ORM class to map rides (as definied in the data file)  into a Python Object. """

    __tablename__ = "ecobici_rides"

    id = Column(Integer, primary_key=True)
    user_genre = Column(String)
    user_age = Column(Integer)
    bike_id = Column(Integer)
    start_station_id = Column(Integer)
    start_datetime = Column(DateTime)
    end_station_id = Column(Integer)
    end_datetime = Column(DateTime)

    def __init__(self, user_genre, user_age, bike_id, start_station_id, start_datetime, end_station_id, end_datetime):
        """
        """
        self.user_genre = user_genre
        self.user_age = user_age
        self.bike_id = bike_id
        self.start_station_id = start_station_id
        self.start_datetime = start_datetime
        self.end_station_id = end_station_id
        self.end_datetime = end_datetime


def create_tables(engine):
    """
    """
    Base.metadata.create_all(engine)


def process_data(data_dir, engine):
    """
    Reads all the *.csv files in `data_dir`, for each file process it into the database
    """

    Session = sessionmaker(bind=engine)
    session = Session()

    def csv2db(filename):
        """
        Reads `filename`: for each line builds up the data and saves it in database.
        """
        rides = []
        with open(filename, "r", encoding="utf-8") as f:
            next(f)
            reader = csv.reader(f, delimiter=',')
            cont = 0
            for row in reader:
                genre = row[0]
                age = int(row[1])
                bike = int(row[2])
                start_station = int(row[3])
                start_time = datetime.fromtimestamp(pendulum.parse("{}T{}".format(row[4], row[5])).timestamp())
                end_station = int(row[6])
                end_time = datetime.fromtimestamp(pendulum.parse("{}T{}".format(row[7], row[8])).timestamp())

                rides.append(Ride(genre, age, bike, start_station, start_time, end_station, end_time))
                cont += 1
                
                if cont == 2000:
                    # we'll write every 2000 entries into the database.
                    # The amount of entries should be configurable via a settings module.
                    session.add_all(rides)
                    session.commit()
                    rides = []
                    cont = 0

    dirp = os.scandir(data_dir)
    for f in dirp:
        filename = os.path.splitext(f.name)
        if filename[1] == ".csv":
            csv2db(os.path.join(data_dir, f.name))


def main():
    
    engine = create_engine('sqlite:///ecobici.db', echo=True)  # we'll use the SQLite engine for this iteration
    
    create_tables(engine)
    process_data("data/csv", engine)  # the data is hosted in the data/csv directory, should be a setting in an addional module.
