-- With a dba user on a localhost environment
CREATE DATABASE MELI;
USE MELI;
CREATE USER 'guest'@'localhost' IDENTIFIED BY 'guest';
CREATE TABLE meli.best_seller_tbl
(
	item_id varchar(20),
	seller_id varchar(20),
	sales_qty integer,
	price double,
	currency varchar(10),
    ratio double,
	warranty varchar(100),
	shipping varchar(100),
	process_dt date,
	primary key (item_id, seller_id, process_dt)
);
grant select, insert, delete, update on meli.best_seller_tbl to guest@'localhost';
CREATE TABLE meli.process_timing
(
	SYS_DT DATETIME,
    API_TIME DOUBLE,
    API_NAME VARCHAR(10),
    DB_TIME DOUBLE,
    COMPLETE_PROC_TIME DOUBLE,
    PROCESS_ROWS INTEGER,
	PRIMARY KEY (API_NAME,SYS_DT)
);
grant select, insert, delete, update on meli.process_timing to guest@'localhost';
 




