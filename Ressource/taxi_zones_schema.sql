

 CREATE TABLE public.Taxi_Zone (
    id integer NOT NULL,
    Borough varchar(255) NULL,
    Zon    varchar(255) NULL,
    Service_zone varchar(255) NULL,
    PRIMARY KEY (id)

);


ALTER TABLE public.Taxi_Zone OWNER TO kenne;

CREATE SEQUENCE public.Taxi_Zone_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.Taxi_Zone_id_seq OWNER TO kenne;
ALTER SEQUENCE public.Taxi_Zone_id_seq OWNED BY public.Taxi_Zone.id;

ALTER TABLE ONLY public.Taxi_Zone ALTER COLUMN id SET DEFAULT nextval('public.Taxi_Zone_id_seq'::regclass);