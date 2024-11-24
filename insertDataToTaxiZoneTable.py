from insert_data_to_db import *

Taxi_zone_DF = (
      spark
        .read
        .csv("./file_csv/TaxiZones.csv")
)

Taxi_zone_DF.show(4)