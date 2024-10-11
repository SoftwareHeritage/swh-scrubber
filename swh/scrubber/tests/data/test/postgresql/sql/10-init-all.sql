--
-- PostgreSQL database dump
--

-- Dumped from database version 16.0 (Debian 16.0-1.pgdg120+1)
-- Dumped by pg_dump version 16.0 (Debian 16.0-1.pgdg120+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: datastore_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.datastore_type AS ENUM (
    'storage',
    'journal',
    'objstorage'
);


--
-- Name: object_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.object_type AS ENUM (
    'content',
    'directory',
    'revision',
    'release',
    'snapshot',
    'extid',
    'raw_extrinsic_metadata'
);


--
-- Name: swhid; Type: DOMAIN; Schema: public; Owner: -
--

CREATE DOMAIN public.swhid AS text
	CONSTRAINT swhid_check CHECK ((VALUE ~ '^swh:[0-9]+:.*'::text));


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: check_config; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.check_config (
    id integer NOT NULL,
    datastore integer NOT NULL,
    object_type public.object_type NOT NULL,
    nb_partitions bigint NOT NULL,
    name text,
    comment text
);


--
-- Name: check_config_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.check_config_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: check_config_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.check_config_id_seq OWNED BY public.check_config.id;


--
-- Name: checked_partition; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.checked_partition (
    config_id integer NOT NULL,
    partition_id bigint NOT NULL,
    start_date timestamp with time zone,
    end_date timestamp with time zone
);


--
-- Name: corrupt_object; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.corrupt_object (
    id public.swhid NOT NULL,
    datastore integer NOT NULL,
    object bytea NOT NULL,
    first_occurrence timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: datastore; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.datastore (
    id integer NOT NULL,
    package public.datastore_type NOT NULL,
    class text,
    instance text
);


--
-- Name: datastore_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.datastore_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: datastore_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.datastore_id_seq OWNED BY public.datastore.id;


--
-- Name: fixed_object; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fixed_object (
    id public.swhid NOT NULL,
    object bytea NOT NULL,
    method text,
    recovery_date timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: missing_object; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.missing_object (
    id public.swhid NOT NULL,
    datastore integer NOT NULL,
    first_occurrence timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: missing_object_reference; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.missing_object_reference (
    missing_id public.swhid NOT NULL,
    reference_id public.swhid NOT NULL,
    datastore integer NOT NULL,
    first_occurrence timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: object_origin; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.object_origin (
    object_id public.swhid NOT NULL,
    origin_url text NOT NULL,
    last_attempt timestamp with time zone
);


--
-- Name: check_config id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.check_config ALTER COLUMN id SET DEFAULT nextval('public.check_config_id_seq'::regclass);


--
-- Name: datastore id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datastore ALTER COLUMN id SET DEFAULT nextval('public.datastore_id_seq'::regclass);


--
-- Data for Name: check_config; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.check_config (id, datastore, object_type, nb_partitions, name, comment) FROM stdin;
\.


--
-- Data for Name: checked_partition; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.checked_partition (config_id, partition_id, start_date, end_date) FROM stdin;
\.


--
-- Data for Name: corrupt_object; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.corrupt_object (id, datastore, object, first_occurrence) FROM stdin;
\.


--
-- Data for Name: datastore; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.datastore (id, package, class, instance) FROM stdin;
\.


--
-- Data for Name: fixed_object; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.fixed_object (id, object, method, recovery_date) FROM stdin;
\.


--
-- Data for Name: missing_object; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.missing_object (id, datastore, first_occurrence) FROM stdin;
\.


--
-- Data for Name: missing_object_reference; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.missing_object_reference (missing_id, reference_id, datastore, first_occurrence) FROM stdin;
\.


--
-- Data for Name: object_origin; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.object_origin (object_id, origin_url, last_attempt) FROM stdin;
\.


--
-- Name: check_config_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.check_config_id_seq', 1, false);


--
-- Name: datastore_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.datastore_id_seq', 1, false);


--
-- Name: check_config check_config_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.check_config
    ADD CONSTRAINT check_config_pkey PRIMARY KEY (id);


--
-- Name: checked_partition checked_partition_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.checked_partition
    ADD CONSTRAINT checked_partition_pkey PRIMARY KEY (config_id, partition_id);


--
-- Name: corrupt_object corrupt_object_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.corrupt_object
    ADD CONSTRAINT corrupt_object_pkey PRIMARY KEY (id, datastore);


--
-- Name: datastore datastore_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.datastore
    ADD CONSTRAINT datastore_pkey PRIMARY KEY (id);


--
-- Name: fixed_object fixed_object_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fixed_object
    ADD CONSTRAINT fixed_object_pkey PRIMARY KEY (id);


--
-- Name: missing_object missing_object_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.missing_object
    ADD CONSTRAINT missing_object_pkey PRIMARY KEY (id, datastore);


--
-- Name: check_config_unicity_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX check_config_unicity_idx ON public.check_config USING btree (datastore, object_type, nb_partitions);


--
-- Name: datastore_package_class_instance; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX datastore_package_class_instance ON public.datastore USING btree (package, class, instance);


--
-- Name: missing_object_reference_missing_id_reference_id_datastore; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX missing_object_reference_missing_id_reference_id_datastore ON public.missing_object_reference USING btree (missing_id, reference_id, datastore);


--
-- Name: missing_object_reference_reference_id_missing_id_datastore; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX missing_object_reference_reference_id_missing_id_datastore ON public.missing_object_reference USING btree (reference_id, missing_id, datastore);


--
-- Name: object_origin_by_origin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX object_origin_by_origin ON public.object_origin USING btree (origin_url, object_id);


--
-- Name: object_origin_pkey; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX object_origin_pkey ON public.object_origin USING btree (object_id, origin_url);


--
-- Name: corrupt_object corrupt_object_datastore_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.corrupt_object
    ADD CONSTRAINT corrupt_object_datastore_fkey FOREIGN KEY (datastore) REFERENCES public.datastore(id);


--
-- Name: missing_object missing_object_datastore_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.missing_object
    ADD CONSTRAINT missing_object_datastore_fkey FOREIGN KEY (datastore) REFERENCES public.datastore(id);


--
-- Name: missing_object_reference missing_object_reference_datastore_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.missing_object_reference
    ADD CONSTRAINT missing_object_reference_datastore_fkey FOREIGN KEY (datastore) REFERENCES public.datastore(id);


--
-- PostgreSQL database dump complete
--
