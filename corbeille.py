"""

       # avec
    # joindre les partitions_col
    df.createOrReplaceTempView("taxi_yellow")

    spark.sql("""
SELECT
TripDays, COUNT(passenger_count)
AS
passeneger_count
FROM
taxi_yellow
GROUP
by
TripDays
""").show(10)


# identification des jours de pointe
df2 = spark.sql(
"""
SELECT
TripDays,
SUM(tripdistance)
AS
total_trip_distance,
SUM(passenger_count)
AS
totalPassenger
FROM
taxi_yellow
GROUP
BY
TripDays
"""
)

# (SUM(tripdistance) + SUM(passenger_count)) / 2 AS resultat,


# identification heure de pointe
df_hours = spark.sql(
"""
SELECT
tpep_pickup_datetime, SUM(passenger_count)
AS
total_passenger
FROM
taxi_yellow
GROUP
BY
tpep_pickup_datetime
ORDER
BY
total_passenger
DESC
"""
)
"""