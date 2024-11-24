from pyspark.sql.types import StructType, IntegerType, StructField, TimestampType, DoubleType, StringType
""" 
yellowTaxiShema = (
                     StructType
                     ([

                             StructField("VendorId"                  , IntegerType()     ,True),
                             StructField("lpep_pickup_datetime"      , TimestampType()   , True),
                             StructField("lpep_dropoff_datetime"     , TimestampType()    ,True),
                             StructField("passenger_count"           , DoubleType()       , True),
                             StructField("trip_distance"             ,  DoubleType()      , True),
                             StructField("state"                     , StringType()      , True),
                             StructField("PULocationID"               , IntegerType()   , True),
                             StructField("DOLocationID"             ,  IntegerType()     , True),
                             StructField("payment_type"               , IntegerType()      , True),
                             StructField("fare_amount"               , DoubleType()         , True),
                             StructField("extra"                    , DoubleType()          , True),
                             StructField("mta_tax"                  , DoubleType()         , True),
                             StructField("tip_amount"                , DoubleType()        , True),
                             StructField("tolls_amount"              , DoubleType()         , True),
                             StructField("total_amount"              , DoubleType()          , True),
                             StructField("congestion_surcharge"       , DoubleType()         , True),
                             StructField("airport_fee"                , DoubleType()          , True),


                      ])
                  )
"""
taxi_zone_df_schema = (
        StructType
                ([

                StructField("id", IntegerType(), False),
                StructField("Borough", StringType(), True),
                StructField("Zon", StringType(), True),
                StructField("Service_zone", StringType(), True),

        ])
)






