# Verify SparkContext
print(sc)

# Print Spark version
print(sc.version)

############################################
#You can think of the SparkContext as your connection to the cluster and the SparkSession as your interface with that connection.

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
#This returns an existing SparkSession if there's already one in the environment, or creates a new one if necessary!
my_spark = SparkSession.builder.getOrCreate() 

# Print my_spark
print(my_spark)

###############################################
#Your SparkSession has an attribute called catalog which lists all the data inside the cluster. This attribute has a few methods for extracting different pieces of information.
#One of the most useful is the .listTables() method, which returns the names of all the tables in your cluster as a list.
# Print the tables in the catalog
print(spark.catalog.listTables())

###########################################
#Running a query on this table is as easy as using the .sql() method on your SparkSession

# Don't change this query
query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
flights10 = spark.sql(query)

# Show the results
flights10.show()

#################################################
#.toPandas() method

# Don't change this query
query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = spark.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts.head())

##############################################3
#From pandas to spark

# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(spark.catalog.listTables())

########################################
#Read csv diretly 

# Don't change this file path
file_path = "/usr/local/share/datasets/airports.csv"

# Read in the airports data
airports = spark.read.csv(file_path, header = True)

# Show the data
airports.show()

########################################
#Creating columns

# Create the DataFrame flights
flights = spark.table("flights")

# Show the head
flights.show()

# Add duration_hrs
flights = flights.withColumn("duration_hrs", flights.air_time / 60)

#################################
##SQL WHERE   -   Spark .filter()

# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)

# Print the data to check they're equal
print(long_flights1.show())
print(long_flights2.show())

##########################################
## SELECT -> .select()

# Select the first set of columns
selected1 = flights.select("tailnum","origin","dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)

##############################################




#######################################################
####################################################
##Recommender systems

# View TJ_ratings
TJ_ratings.show()

# Generate recommendations for users
get_ALS_recs(["Jane","Taylor"]) 

#########################################################

##Ratings data types
# Group the data by "Genre"
markus_ratings.groupBy("Genre").sum().show()

######################################