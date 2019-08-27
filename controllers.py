import pymongo
import pymongo_spark

on_time_dataframe = spark.read.format('com.databricks.spark.csv.options(header='true',treatEmptyValuesAsNulls='true',)/
                                      .load('data/On_Time_On_Time_Performance_2015.csv.bz2'))
                                      
on_time_dataframe.registerTempTable("on_time_performance")

trimmed_cast_performance = spark.sql("""SELECT Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate, Carrier, TailNum, FlightNum,
Origin, OriginCityName, OriginState, Dest, DestCityName, DestState, DepTime, cast(DepDelay as float), cast(DepDelayMinutes as int),
cast(TaxiOut as float), cast(TaxiIn as float), WheelsOff, WheelsOn, ArrTime, cast(ArrDelay as float), cast(ArrDelayMinutes as float),
cast(Cancelled as int), cast(Diverted as int), cast(ActualElapsedTime as float), cast(AirTime as float), cast(Flights as int), 
cast(Distance as float), cast(CarrierDelay as float), cast(WeatherDelay as float), cast(NASDelay as float), cast(SecurityDelay as float),
cast(LateAircraftDelay as float), CRSDepTime, CRSArrTime FROM on_time_performance""")                                     

# Replace on_time_performance table with our new, trimmed table and show its columns
trimmed_cast_performance.registerTempTable("on_time_performance")
#trimmed_cast_performance.show()


trimmed_cast_performance.write.parquet("data/on_time_performance.parquet")
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')                                      


pymongo_spark.activate()

#convert each row to a dict
as_dict = on_time_dataframe.rdd.map(lambda row: row.asDict())

#This collection acts as a base table containing all information about the flights
as_dict.saveToMongoDB('mongodb://localhost:27017/agile_data_science.on_time_performance')

#Create an index that will speed up queries for getting all flights from one airport to another, on a given date
db.on_time_performance.ensureIndex({Origin: 1, Dest: 1, FlightDate: 1})


#Create an airplane entity, identifiable by its tail number

# Filter down to the fields we need to identify and link to a flight
flights = on_time_dataframe.rdd.map(lambda x:(x.Carrier, x.FlightDate, x.FlightNum, x.Origin, x.Dest, x.TailNum))

flights_per_airplane = flights.map(lambda nameTuple: (nameTuple[5], [nameTuple[0:5]]))\
.reduceByKey(lambda a, b: a + b)\
.map(lambda tuple:{'TailNum': tuple[0], 'Flights': sorted(tuple[1], key=lambda x: (x[1], x[2]))})

pymongo_spark.activate()

#This table contains a basic flight history for each airplane
flights_per_airplane.saveToMongoDB('mongodb://localhost:27017/agile_data_science.flights_per_airplane')

#Create an index for the quickly fetching all of the flights for a given airplane (identified by tail number)
db.flights_per_airplane.ensureIndex({"TailNum": 1})


# Get all unique tail numbers for each airline
on_time_dataframe.registerTempTable("on_time_performance")
carrier_airplane = spark.sql("SELECT DISTINCT Carrier, TailNum FROM on_time_performance")
# Now we need to store a sorted list of tail numbers for each carrier, along with a fleet count
airplanes_per_carrier = carrier_airplane.rdd.map(lambda nameTuple: (nameTuple[0], [nameTuple[1]]))\
.reduceByKey(lambda a, b: a + b).map(lambda tuple:
     {'Carrier': tuple[0],'TailNumbers': sorted(filter(lambda x: x != '', tuple[1])),'FleetCount': len(tuple[1])})

#This collection contains the fleet of each airplane carrier
airplanes_per_carrier.saveToMongoDB('mongodb://localhost:27017/agile_data_science.airplanes_per_carrier')

