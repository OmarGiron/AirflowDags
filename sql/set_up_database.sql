CREATE SCHEMA IF NOT EXISTS  movies;

CREATE TABLE IF NOT EXISTS movies.user_purchase 
(
   invoice_number varchar(10),
   stock_code varchar(20) NULL,
   detail varchar(1000) NULL,
   quantity int NULL,
   invoice_date timestamp NULL,
   unit_price numeric(8,3) NULL,
   customer_id int NULL,
   country varchar (20) NULL
);

TRUNCATE TABLE movies.user_purchase;