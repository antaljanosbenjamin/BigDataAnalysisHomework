# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import pyspark.sql.types
from pyspark.sql.types import *
from pyspark.sql.functions import *

routesSchema = StructType([
	StructField('agency_id',StringType(),True),
	StructField('route_id',StringType(),False),
	StructField('route_short_name',StringType(),False),
	StructField('route_long_name',StringType(),False),
	StructField('route_type',IntegerType(),False),
	StructField('route_desc',StringType(),True),
	StructField('route_color',StringType(),True),
	StructField('route_text_color',StringType(),True)])

routes = spark.read.csv('routes.txt', header = True, mode = 'DROPMALFORMED', schema = routesSchema)
routes = routes.select(routes.route_id, routes.route_short_name)
routes = routes.filter('route_short_name = 7')

tripsSchema = StructType([
	StructField('route_id',StringType(),False),
	StructField('trip_id',StringType(),False),
	StructField('service_id',StringType(),False),
	StructField('trip_headsign',StringType(),True),
	StructField('direction_id',IntegerType(),True),
	StructField('block_id',StringType(),True),
	StructField('shape_id',StringType(),True),
	StructField('wheelchair_accessible',IntegerType(),True),
	StructField('bikes_allowed',IntegerType(),True)])

trips = spark.read.csv('trips.txt', header = True, mode = 'DROPMALFORMED', schema = tripsSchema);
trips = trips.select(trips.route_id, trips.trip_id, trips.service_id)

trips = trips.filter(trips.route_id == routes.first().route_id)

stopsSchema = StructType([
	StructField('stop_id',StringType(),False),
	StructField('stop_name',StringType(),False),
	StructField('stop_lat',DoubleType(),False),
	StructField('stop_lon',DoubleType(),False),
	StructField('stop_code',StringType(),True),
	StructField('location_type',StringType(),True),
	StructField('parent_station',StringType(),True),
	StructField('wheelchair_boarding',IntegerType(),True),
	StructField('stop_direction',IntegerType(),True)])

stops = spark.read.csv('stops.txt', header = True, mode = 'DROPMALFORMED', schema = stopsSchema)
stops = stops.select(stops.stop_id, stops.stop_name, stops.parent_station)

blahaStations = stops.filter(stops.stop_name.like('Blaha Lujza tér%'))
blahaStations = blahaStations.select(blahaStations.stop_id, blahaStations.stop_name)
renamedBlahaStations = blahaStations.withColumnRenamed('stop_id','station_id').withColumnRenamed('stop_name','station_name')
blahaStops = stops.join(renamedBlahaStations, stops.parent_station == renamedBlahaStations.station_id)

blahaStops = blahaStops.select(blahaStops.stop_id, blahaStops.stop_name)
blahaStops = blahaStops.union(blahaStations).distinct()

stopTimesSchema = StructType([
	StructField('trip_id',StringType(),False),
	StructField('stop_id',StringType(),False),
	StructField('arrival_time',StringType(),False),
	StructField('departure_time',StringType(),False),
	StructField('stop_sequence',StringType(),False),
	StructField('shape_dist_traveled',DoubleType(),True)
])

stopTimes = spark.read.csv('stop_times.txt', header = True, mode = 'DROPMALFORMED', schema = stopTimesSchema)
stopTimes = stopTimes.select(stopTimes.trip_id, stopTimes.stop_id)

calendarDatesSchema = StructType([
    StructField('service_id', StringType(), False),
    StructField('date', DateType(), False),
    StructField('exception_type', IntegerType(), False)
])

calendarDates = spark.read.csv(
    'calendar_dates.txt', header = True, mode = 'DROPMALFORMED', schema = calendarDatesSchema, dateFormat='yyyyMMdd')

tripsAndStopTimes = trips.join(stopTimes, stopTimes.trip_id == trips.trip_id)
tripsAndStopsTimesAndStop = tripsAndStopTimes.join(blahaStops, tripsAndStopTimes.stop_id == blahaStops.stop_id)
blahaCountPerService = tripsAndStopsTimesAndStop.groupBy('service_id').count()
blahaCountPerServicePerDay = calendarDates.join(
    blahaCountPerService, blahaCountPerService.service_id == calendarDates.service_id)
countOfStopsAtBlaha = blahaCountPerServicePerDay.groupBy().sum('count').first()['sum(count)']

# <codecell>

feedInfoSchema = StructType([
    StructField('publisher', StringType(), False),
    StructField('publisher_url', StringType(), False),
    StructField('feed_lang', StringType(), False),
    StructField('start_date', DateType(), False),
    StructField('end_date', DateType(), False),
    StructField('version', StringType(), False),
    StructField('ext_version', StringType(), False)
])

feedInfo = spark.read.csv(
    'feed_info.txt', header = True, mode = 'DROPMALFORMED', schema = feedInfoSchema, dateFormat='yyyyMMdd')
days = (feedInfo.first()['end_date'] - feedInfo.first()['start_date']).days

# <codecell>

print countOfStopsAtBlaha
print days
print '%.2f' % ((countOfStopsAtBlaha * 1.0) / days)

