#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('matplotlib', 'inline')
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt


# In[2]:


import numpy as np
import pandas as pd


# In[3]:


import datetime as dt
# Use timedelta to express "...difference between two date, time, or datetime instances to microsecond resolution."(https://docs.python.org/2/library/datetime.html)
from datetime import timedelta


# # Reflect Tables into SQLAlchemy ORM

# In[4]:


# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import inspect
from flask import Flask, jsonify


# In[5]:


engine = create_engine("sqlite:///Resources/hawaii.sqlite", echo = False)


# In[6]:


# reflect an existing database into a new model
e_db = automap_base()
# reflect the tables
e_db.prepare(engine, reflect=True)


# In[7]:


# We can view all of the classes that automap found
e_db.classes.keys()


# In[8]:


# Save references to each table
measurement = e_db.classes.measurement
station = e_db.classes.station


# In[9]:


# Create our session (link) from Python to the DB
session = Session(engine)


# In[10]:


# Declare and assign variable to utilize inspect function on engine variable containing data
inspecting = inspect(engine)
# Relection
inspecting.get_table_names()


# In[11]:


# Reflection
columns = inspecting.get_columns('station')
# Commence for loop to iterate through name and type  of data in station table
for column in columns:
    print(column['name'], column['type'])


# In[12]:


# Reflection
columns = inspecting.get_columns('measurement')
# Commence for loop to iterate through name and type of data in measurement table
for column in columns:
    print (column['name'], column['type'])


# In[13]:


# Generate list of Hawain cities with respective additional data
engine.execute('SELECT * From station LIMIT 10').fetchall()


# In[14]:


# Generate list of Hawain cities with respective additional data pertaining to measurement table
engine.execute('SELECT * FROM measurement LIMIT 10').fetchall()


# In[15]:


# Query to declare and assign earliest measurement
earliest_string = earliest_string = session.query(measurement.date).order_by(measurement.date).first()
# Print earliest
print(f"Earliest: {earliest_string[0]}")


# In[16]:


# Query to declare and assign latest measurement
latest_string = latest_string = session.query(measurement.date).order_by(measurement.date.desc()).first()
print(f"Latest: {latest_string[0]}")


# # Exploratory Climate Analysis

# In[17]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results
latest_date = dt.datetime.strptime(latest_string[0], '%Y-%m-%d')
# Calculate the date 1 year ago from the last data point in the database
year_prior = dt.date(latest_date.year -1, latest_date.month, latest_date.day)
year_prior
# Perform a query to retrieve the data and precipitation scores
d_prcp = [measurement.date,measurement.prcp]
query = session.query(*d_prcp).filter(measurement.date >= year_prior).all()
# Save the query results as a Pandas DataFrame and set the index to the date column
# Set index of pandas df to date
precip = pd.DataFrame(query, columns=['Date','Precipitation'])
# Clean data of NA(s)
precip = precip.dropna(how='any') 
# Put dates in ascending order
precip = precip.sort_values(["Date"], ascending=True)
precip = precip.set_index("Date")
# Produce head of precip
precip.head()
#precip.tail()


# In[18]:


# Use Pandas Plotting with Matplotlib to plot the data
precip.plot(figsize = (20,15), 
title = "Hawaiian Precip: 2016-08-23 To 2017-08-23",
sort_columns = True,
use_index = True,
rot = 60,
legend = True,
fontsize = 24,
color = "aqua")


# In[19]:


# Use Pandas to calcualte the summary statistics for the precipitation data
precip.describe()


# In[20]:


# Design a query to show how many stations are available in this dataset?
stations_query = session.query(func.count(station.station)).all()
print(stations_query)


# In[21]:


# What are the most active stations? (i.e. what stations have the most rows)?
query_rows = [measurement.station,func.count(measurement.id)]
# List the stations and the counts in descending order.
active_stations = session.query(*query_rows).group_by(measurement.station).order_by(func.count(measurement.id).desc()).all()
active_stations


# In[22]:


# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature of the most active station?
query_rows = [func.min(measurement.tobs),func.max(measurement.tobs),func.avg(measurement.tobs)]
most_active_station = session.query(*query_rows).group_by(measurement.station).order_by(func.count(measurement.id).desc()).first()
most_active_station


# In[23]:


# Choose the station with the highest number of temperature observations.
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram

x_query = session.query(measurement.tobs).    filter(measurement.station == active_stations[0][0]).    filter(measurement.date >= year_prior).all()
temperatures = list(np.ravel(x_query))

y_query = [station.station,station.name,station.latitude,station.longitude,station.elevation]
x_query = session.query(*y_query).all()
stations_desc = pd.DataFrame(x_query, columns=['station','Name','Latitude','Longitude','Elevation'])

stationname = stations_desc.loc[stations_desc["station"] == active_stations[0][0],"Name"].tolist()[0]

# n, bins, patches = plt.hist(temperatures, bins=12,alpha=0.7, rwidth=1.0,label='tobs')
plt.hist(temperatures, bins=12,rwidth=1.25,label='tobs', color = "b", edgecolor = "r")
plt.grid(axis="both", alpha=1.00)
plt.xlabel("Temperature")
plt.ylabel("Frequency")
plt.title(f"Temperatures Taken From {year_prior} To {latest_string[0]} \nIn {stationname}")
plt.legend()


# In[24]:


get_ipython().system('jupyter nbconvert --to=python')


# ## Bonus Challenge Assignment

# In[25]:


# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

# function usage example
print(calc_temps('2012-02-28', '2012-03-05'))


# In[ ]:


# Use your previous function `calc_temps` to calculate the tmin, tavg, and tmax 
# for your trip using the previous year's data for those same dates.


# In[ ]:


# Plot the results from your previous query as a bar chart. 
# Use "Trip Avg Temp" as your Title
# Use the average temperature for the y value
# Use the peak-to-peak (tmax-tmin) value as the y error bar (yerr)


# In[ ]:


# Calculate the total amount of rainfall per weather station for your trip dates using the previous year's matching dates.
# Sort this in descending order by precipitation amount and list the station, name, latitude, longitude, and elevation


# In[ ]:


# Create a query that will calculate the daily normals 
# (i.e. the averages for tmin, tmax, and tavg for all historic data matching a specific month and day)

def daily_normals(date):
    """Daily Normals.
    
    Args:
        date (str): A date string in the format '%m-%d'
        
    Returns:
        A list of tuples containing the daily normals, tmin, tavg, and tmax
    
    """
    
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    return session.query(*sel).filter(func.strftime("%m-%d", Measurement.date) == date).all()
    
daily_normals("01-01")


# In[ ]:


# calculate the daily normals for your trip
# push each tuple of calculations into a list called `normals`

# Set the start and end date of the trip

# Use the start and end date to create a range of dates

# Stip off the year and save a list of %m-%d strings

# Loop through the list of %m-%d strings and calculate the normals for each date


# In[ ]:


# Load the previous query results into a Pandas DataFrame and add the `trip_dates` range as the `date` index


# In[ ]:


# Plot the daily normals as an area plot with `stacked=False`

