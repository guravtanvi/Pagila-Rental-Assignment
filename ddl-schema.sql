-- Creating the destination database

-- Database: rentaldb

-- DROP DATABASE rentaldb;

CREATE DATABASE rentaldb
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

-- Creating the table for aggregated data

-- Table: public.rental

-- DROP TABLE public.rental;

CREATE TABLE public.rental
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    weekbeginning date NOT NULL,
    outstandingrentals integer,
    returnedrentals integer,
    last_update timestamp with time zone NOT NULL DEFAULT now()
)

TABLESPACE pg_default;

ALTER TABLE public.rental
    OWNER to postgres;
