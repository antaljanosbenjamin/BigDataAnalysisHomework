# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import abs, udf, col

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
routes = routes.select(routes.route_id, routes.route_short_name, routes.route_type)

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
trips = trips.select(trips.route_id, trips.trip_id, trips.service_id, trips.direction_id)

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
stops = stops.select(stops.stop_id, stops.stop_name,
    stops.parent_station, stops.location_type, stops.stop_lat, stops.stop_lon)

def fillParentStation(stop_id, parent_station):
  if parent_station == None:
    return stop_id
  else:
    return parent_station

fillParentStationUDF = udf(fillParentStation)
addRealParent = [c for c in ["*"]] + [fillParentStationUDF(col("stop_id"),
    col("parent_station")).alias("real_parent_station")]
stopsWithRealParent = stops.select(*addRealParent)


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

# <headingcell level=1>

# FUTAR-01 - DONE

# <markdowncell>

# Van-e olyan buszjárat, ahol két különböző megálló neve egyazon járaton belül megegyezik (és melyek ezek, ha van)? Pontosítás: Oda-vissza járatban természetesen nem számít. Egyazon irányba menet szerepel-e ugyanolyan nevű megálló

# <codecell>

stopsAndStopTimes = stopsWithRealParent.join(stopTimes, stopTimes.stop_id == stopsWithRealParent.stop_id)
stopsAndStopTimesAndTrips = stopsAndStopTimes.join(trips, trips.trip_id == stopsAndStopTimes.trip_id)
stopNamesAndStopIdsAndTrips = stopsAndStopTimesAndTrips.select(stops.stop_name,
    stopsWithRealParent.real_parent_station, trips.trip_id, trips.direction_id).distinct()
stopNamesCountByTripId = stopNamesAndStopIdsAndTrips.groupBy(
    'trip_id','stop_name','direction_id').count().withColumnRenamed('count','stop_name_count')
stopNamesCountByTripId.filter('stop_name_count >= 2').orderBy(stopNamesCountByTripId.stop_name_count.desc()).show()

# <headingcell level=1>

# FUTAR-02 - DONE

# <markdowncell>

# Egy nap hányszor áll meg 7-es busz a Blaha Lujza téren?
# Pontosítás: átlagosan per nap.

# <codecell>

routesOnly7 = routes.filter('route_short_name = 7')

tripsOnly7 = trips.filter(trips.route_id == routesOnly7.first().route_id)

blahaStations = stops.filter(stops.stop_name.like('Blaha Lujza tér%'))
blahaStations = blahaStations.select(blahaStations.stop_id, blahaStations.stop_name)
blahaStationsRenamedColumns = blahaStations.withColumnRenamed(
    'stop_id','station_id').withColumnRenamed('stop_name','station_name')

blahaStops = stops.join(blahaStationsRenamedColumns, stops.parent_station == blahaStationsRenamedColumns.station_id)
blahaStops = blahaStops.select(blahaStops.stop_id, blahaStops.stop_name)
blahaStops = blahaStops.union(blahaStations).distinct()

tripsAndStopTimes = tripsOnly7.join(stopTimes, stopTimes.trip_id == tripsOnly7.trip_id)

tripsAndStopsTimesAndStops = tripsAndStopTimes.join(blahaStops, tripsAndStopTimes.stop_id == blahaStops.stop_id)

blahaCountPerService = tripsAndStopsTimesAndStops.groupBy('service_id').count()

blahaCountPerServicePerDay = calendarDates.join(
    blahaCountPerService, blahaCountPerService.service_id == calendarDates.service_id)
countOfStopsAtBlaha = blahaCountPerServicePerDay.groupBy().sum('count').first()['sum(count)']

days = (feedInfo.first()['end_date'] - feedInfo.first()['start_date']).days

print '%.2f' % ((countOfStopsAtBlaha * 1.0) / days)

# <headingcell level=1>

# FUTAR-05 - DONE

# <markdowncell>

# Hány különböző megállója van összesen a BKK-hajóknak?

# <codecell>

shipRoutesOnly = routes.filter('route_type = 4')
shipTrips = shipRoutesOnly.join(trips, shipRoutesOnly.route_id == trips.route_id)
shipStopTimes = shipTrips.join(stopTimes, stopTimes.trip_id == shipTrips.trip_id)
shipStopTimesAndStops = shipStopTimes.join(stopsWithRealParent, stopsWithRealParent.stop_id == shipStopTimes.stop_id)
shipStopCounts = shipStopTimesAndStops.select(
    shipStopTimesAndStops.real_parent_station).distinct().groupBy().count().first()['count']
print '%d' % shipStopCounts

# <headingcell level=1>

# FUTAR-06 - DONE

# <markdowncell>

# Vannak-e azonos nevű megállók, amelyek messzebb vannak egymástól mint 0.0025 szélességi fok vagy 0.0025 hosszúsági fok?!

# <codecell>

rightStops = stops.select('stop_name','stop_id','stop_lon','stop_lat').withColumnRenamed(
    'stop_name','r_name').withColumnRenamed('stop_id','r_id').withColumnRenamed(
    'stop_lon','r_lon').withColumnRenamed('stop_lat', 'r_lat')

leftStops = stops.select('stop_name','stop_id','stop_lon','stop_lat')

stopsWithSameName = leftStops.join(rightStops, (leftStops.stop_name == rightStops.r_name )
    & ~(leftStops.stop_id < rightStops.r_id) 
    & ((abs(rightStops.r_lat - leftStops.stop_lat) > 0.0025) | (abs(rightStops.r_lon - leftStops.stop_lon) > 0.0025) ))

stopsWithSameNameAndDistances = stopsWithSameName.withColumn(
    'lat_dist',abs(rightStops.r_lat - leftStops.stop_lat)).withColumn(
    'lon_dist',abs(rightStops.r_lon - leftStops.stop_lon))
stopsWithSameNameAndDistances.orderBy(stopsWithSameNameAndDistances.lon_dist.desc()).orderBy(
    stopsWithSameNameAndDistances.lat_dist.desc()).show()

# <headingcell level=1>

# FUTAR-08 - DONE

# <markdowncell>

# Melyik a legtöbb megállót tartalmazó BKK-járat (route)?

# <codecell>

routesAndTrips = routes.join(trips, trips.route_id == routes.route_id)
routesAndTripsAndStopTimes = routesAndTrips = routesAndTrips.join(stopTimes, stopTimes.trip_id == routesAndTrips.trip_id)
routesAndTripsAndStopTimesAndStops = routesAndTripsAndStopTimes.join(stopsWithRealParent, 
    stopsWithRealParent.stop_id == routesAndTripsAndStopTimes.stop_id)
routesAndStopIds = routesAndTripsAndStopTimesAndStops.select(routes.route_short_name,
    routes.route_id,routesAndTripsAndStopTimesAndStops.real_parent_station)
routesAndStopCounts = routesAndStopIds.distinct().groupBy(routesAndStopIds.route_short_name,
    routesAndStopIds.route_id).count()
routesAndStopCounts = routesAndStopCounts.withColumnRenamed('count','stop_count')
routesAndStopCounts.cache()
maxStopCount = routesAndStopCounts.groupBy().max('stop_count').first()['max(stop_count)']
routesAndStopCounts.filter(routesAndStopCounts.stop_count == maxStopCount).show()

