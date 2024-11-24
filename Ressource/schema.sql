CREATE TABLE public.taxi (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    description character varying(255) NOT NULL,
    PRIMARY KEY (id)
);

ALTER TABLE public.taxi OWNER TO kenne;

CREATE SEQUENCE public.taxi_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.taxi_id_seq OWNER TO kenne;
ALTER SEQUENCE public.taxi_id_seq OWNED BY public.taxi.id;

ALTER TABLE ONLY public.taxi ALTER COLUMN id SET DEFAULT nextval('public.taxi_id_seq'::regclass);





