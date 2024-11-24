

 CREATE TABLE public.yellow_taxi_partition_1 (
    id integer NOT NULL,
    VendorID integer NOT NULL,
    tpep_pickup_datetime timestamp NULL,
    tpep_dropoff_datetime timestamp NULL,
    passenger_count integer NULL,
    trip_distance double precision NULL,
    state varchar(255) NULL,
    PULocationID integer NOT NULL,
    DOLocationID integer  NOT NULL,
    payment_type integer NULL,

    PRIMARY KEY (id)
);


 CREATE TABLE public.yellow_taxi_partition_2 (
    id integer NOT NULL,
    fare_amount double precision NULL,
    extra double precision NULL,
    mta_tax double precision NULL,
    tip_amount double precision NULL,
    tolls_amount double precision NULL,
    total_amount double precision NULL,
    congestion_surcharge double precision NULL,
    airport_fee double precision NULL,
    trip_distance_in_meter double precision NULL,

    PRIMARY KEY (id)
);


ALTER TABLE public.yellow_taxi_partition_1 OWNER TO kenne;

CREATE SEQUENCE public.yellow_taxi_partition_1_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.yellow_taxi_partition_1_id_seq OWNER TO kenne;
ALTER SEQUENCE public.yellow_taxi_partition_1_id_seq OWNED BY public.yellow_taxi_partition_1.id;

ALTER TABLE ONLY public.yellow_taxi_partition_1 ALTER COLUMN id SET DEFAULT nextval('public.yellow_taxi_partition_1_id_seq'::regclass);


ALTER TABLE public.yellow_taxi_partition_2 OWNER TO kenne;

CREATE SEQUENCE public.yellow_taxi_partition_2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.yellow_taxi_partition_2_id_seq OWNER TO kenne;
ALTER SEQUENCE public.yellow_taxi_partition_2_id_seq OWNED BY public.yellow_taxi_partition_2.id;

ALTER TABLE ONLY public.yellow_taxi_partition_2 ALTER COLUMN id SET DEFAULT nextval('public.yellow_taxi_partition_2_id_seq'::regclass);
