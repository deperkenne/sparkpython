 CREATE TABLE public.yellow_taxi_partition_composite (
    id bigint NOT NULL,
    VendorID integer NOT NULL,
    tpep_pickup_datetime timestamp NULL,
    tpep_dropoff_datetime timestamp NULL,
    passenger_count integer NULL,
    trip_distance double precision NULL,
    state varchar(255) NULL,
    PULocationID integer NOT NULL,
    DOLocationID integer  NOT NULL,
    payment_typeID integer NOT NULL,
    fare_amount double precision NULL,
    extra double precision NULL,
    mta_tax double precision NULL,
    tip_amount double precision NULL,
    tolls_amount double precision NULL,
    total_amount double precision NULL,
    congestion_surcharge double precision NULL,
    airport_fee double precision NULL,
    trip_distance_in_meter double precision NULL,
    tripYear integer  NULL,
    tripMonth varchar(255)  NULL,
    tripDays varchar(255) NULL,
    tripTime varchar(255) NULL,
    improvement_surcharge double precision NULL,
    RatecodeID double precision NULL,


    PRIMARY KEY (id, tpep_pickup_datetime,payment_typeID)
) PARTITION BY RANGE (tpep_pickup_datetime);

CREATE TABLE public.yellow_taxi_2024 PARTITION OF public.yellow_taxi_partition_composite
    FOR VALUES FROM ('2023-01-01') TO ('2023-12-31')
    PARTITION BY HASH (payment_typeID);

CREATE TABLE public.yellow_taxi_2024_hash_0 PARTITION OF public.yellow_taxi_2024
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE public.yellow_taxi_2024_hash_1 PARTITION OF public.yellow_taxi_2024
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);

CREATE TABLE public.yellow_taxi_2024_hash_2 PARTITION OF public.yellow_taxi_2024
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);

CREATE TABLE public.yellow_taxi_2024_hash_3 PARTITION OF public.yellow_taxi_2024
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);





ALTER TABLE public.yellow_taxi_partition_composite OWNER TO kenne;

CREATE SEQUENCE public.yellow_taxi_partition_composite_id_seq
    AS bigint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.yellow_taxi_partition_composite_id_seq OWNER TO kenne;
ALTER SEQUENCE public.yellow_taxi_partition_composite_id_seq OWNED BY public.yellow_taxi_partition_composite.id;

ALTER TABLE ONLY public.yellow_taxi_partition_composite ALTER COLUMN id SET DEFAULT nextval('public.yellow_taxi_partition_composite_id_seq'::regclass);
ALTER TABLE public.yellow_taxi_2024_hash_0
ALTER COLUMN id SET DEFAULT nextval('public.yellow_taxi_partition_composite_id_seq');

ALTER TABLE public.yellow_taxi_2024_hash_1
ALTER COLUMN id SET DEFAULT nextval('public.yellow_taxi_partition_composite_id_seq');

ALTER TABLE public.yellow_taxi_2024_hash_2
ALTER COLUMN id SET DEFAULT nextval('public.yellow_taxi_partition_composite_id_seq');

ALTER TABLE public.yellow_taxi_2024_hash_3
ALTER COLUMN id SET DEFAULT nextval('public.yellow_taxi_partition_composite_id_seq');



