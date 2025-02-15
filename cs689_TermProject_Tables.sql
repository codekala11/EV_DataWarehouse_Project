CREATE DATABASE CS689_PROJECT;
USE CS689_PROJECT;

--Staging Area
--data from df coming from python
--Pop_df
CREATE TABLE pop_df
(
sequence INT,
filter INT,
county VARCHAR(55),
jurisdiction VARCHAR(55),
pop_2017 INT,
pop_2018 INT,
pop_2019 INT,
pop_2020 INT,
pop_2021 INT,
pop_2022 INT,
pop_2023 INT,
pop_2024 INT,
);

select * from pop_df;
UPDATE pop_df
SET jurisdiction = CASE
    WHEN jurisdiction LIKE 'Unincorporated%' THEN 'Unincorporated'
    ELSE 'Incorporated'
END;


--ev_df
CREATE TABLE ev_df
(
VIN VARCHAR(55),
county VARCHAR(55),
city VARCHAR(55),
state VARCHAR(55),
zip INT,
model_year INT,
make VARCHAR(55),
model VARCHAR(55),
EV_type VARCHAR(55),
CAFV_eligibility VARCHAR(110),
range INT,
MSRP INT
);
select * from ev_df;

UPDATE ev_df
SET CAFV_eligibility = CASE
                           WHEN CAFV_eligibility LIKE '%eligible' THEN 'eligible'
                           ELSE 'non eligible'
                       END;

select * from ev_df;

--ev_pop
CREATE TABLE ev_pop
(
date DATETIME,
county VARCHAR(55),
state VARCHAR(55),
primary_use VARCHAR(55),
BEV INT,
PHEV INT,
EV INT,
Non_EV INT,
Total_vehicles INT,
percent_EV DECIMAL(5, 2)
);

select * from ev_pop;

--dim table staging tables
--county_dim
CREATE TABLE dim_county_staging
(
county_id INT PRIMARY KEY,
county VARCHAR(55),
prev_county_name VARCHAR(55)
);

--date_dim
CREATE TABLE dim_date_staging
(
date_id INT PRIMARY KEY,
full_date DATE,
year INT,
month INT,
day INT,
season VARCHAR(15),
eff_startdate DATE,
eff_enddate DATE,
current_flag Bit
);

--state_dim
CREATE TABLE dim_state_staging
(
state_id INT PRIMARY KEY,
state VARCHAR(15),
state_abbvr VARCHAR(5)
);

--vehicle_type_dim
CREATE TABLE dim_vehicle_type_staging
(
vehicle_type_id INT PRIMARY KEY,
vehicle_type VARCHAR(15)
);

--dim_jurisdiction
CREATE TABLE dim_jurisdiction_staging
(
jurisdiction_id INT PRIMARY KEY,
jurisdiction VARCHAR(25)
);

--dim_year_lookup
CREATE TABLE dim_year_lookup_staging
(
year_id INT PRIMARY KEY,
year INT
);


--staging fact tables

--fact table staging tables
--ev_usage_fact
CREATE TABLE fact_ev_usage_staging
(
date_id INT,
county_id INT,
state_id INT,
vehicle_type_id INT,
PHEV_count INT,
BEV_count INT,
EV_count INT,
FOREIGN KEY (date_id) REFERENCES dim_date_staging(date_id),
FOREIGN KEY (county_id) REFERENCES dim_county_staging(county_id),
FOREIGN KEY (state_id) REFERENCES dim_state_staging(state_id),
FOREIGN KEY (vehicle_type_id) REFERENCES dim_vehicle_type_staging(vehicle_type_id)
);


--pop_ev_fact
CREATE TABLE fact_pop_ev_staging
(
date_id INT,
county_id INT,
state_id INT,
jurisdiction_id INT,
PHEV_count INT,
BEV_count INT,
EV_count INT,
Non_ev_count INT,
total_count INT,
percent_ev DECIMAL(5, 2)
);


--vehicle_detail_fact
CREATE TABLE fact_vehicle_detail_staging
(
VIN VARCHAR(25),
date_id INT,
county_id INT,
state_id INT,
model_year INT,
maker VARCHAR(25),
model VARCHAR(25),
range INT,
MSRP INT,
CAFV_eligibility VARCHAR(25),
Prev_CAFV VARCHAR(25),
transac_load DATETIME,
transac_end DATETIME,
valid_startdate DATE,
valid_end_date DATE
);


--population_fact
CREATE TABLE fact_population_staging
(
pop_id INT,
year_id INT,
county_id INT,
jurisdiction_id INT,
year INT,
total_pop INT,
eff_startdate DATE,
eff_enddate DATE
);



--Data Warehouse
--dim tables

--county_dim
CREATE TABLE dim_county
(
dim_county_id INT IDENTITY(1,1) PRIMARY KEY,
county_id INT,
county VARCHAR(55),
prev_county_name VARCHAR(55)
);

--date_dim
CREATE TABLE dim_date
(
dim_date_id INT IDENTITY(1,1) PRIMARY KEY,
date_id INT,
full_date DATE,
year INT,
month INT,
day INT,
season VARCHAR(15),
eff_startdate DATE,
eff_enddate DATE,
current_flag Bit
);

--state_dim
CREATE TABLE dim_state
(
dim_state_id INT IDENTITY(1,1) PRIMARY KEY,
state_id INT,
state VARCHAR(15),
state_abbvr VARCHAR(5)
);

--vehicle_type_dim
CREATE TABLE dim_vehicle_type
(
dim_vehicle_type_id INT IDENTITY(1,1) PRIMARY KEY,
vehicle_type_id INT,
vehicle_type VARCHAR(15)
);

--dim_jurisdiction
CREATE TABLE dim_jurisdiction
(
dim_jurisdiction_id INT IDENTITY(1,1) PRIMARY KEY,
jurisdiction_id INT,
jurisdiction VARCHAR(25)
);

--dim_year_lookup
CREATE TABLE dim_year_lookup
(
dim_year_id INT IDENTITY(1,1) PRIMARY KEY,
year_id INT,
year INT
);


--fact tables
--EV_USAGE FACT
CREATE TABLE fact_ev_usage
(
dim_date_id INT,
dim_county_id INT,
dim_state_id INT,
dim_vehicle_type_id INT,
date_id INT,
county_id INT,
state_id INT,
vehicle_type_id INT,
PHEV_count INT,
BEV_count INT,
EV_count INT,
FOREIGN KEY (dim_date_id) REFERENCES dim_date(dim_date_id),
FOREIGN KEY (dim_county_id) REFERENCES dim_county(dim_county_id),
FOREIGN KEY (dim_state_id) REFERENCES dim_state(dim_state_id),
FOREIGN KEY (dim_vehicle_type_id) REFERENCES dim_vehicle_type(dim_vehicle_type_id)
);


--pop_ev_fact
-- Create fact_pop_ev table
CREATE TABLE fact_pop_ev
(
    dim_date_id INT,
    dim_county_id INT,
    dim_state_id INT,
    dim_jurisdiction_id INT,
    PHEV_count INT,
    BEV_count INT,
    EV_count INT,
    Non_ev_count INT,
    total_count INT,
    percent_ev DECIMAL(5, 2),
    -- Foreign Key Constraints
    FOREIGN KEY (dim_date_id) REFERENCES dim_date(dim_date_id),
    FOREIGN KEY (dim_county_id) REFERENCES dim_county(dim_county_id),
    FOREIGN KEY (dim_state_id) REFERENCES dim_state(dim_state_id),
    FOREIGN KEY (dim_jurisdiction_id) REFERENCES dim_jurisdiction(dim_jurisdiction_id)
);



--vehicle_detail_fact
-- Create fact_vehicle_detail table
CREATE TABLE fact_vehicle_detail
(
    VIN VARCHAR(25),
    dim_date_id INT,
    dim_county_id INT,
    dim_state_id INT,
    model_year INT,
    maker VARCHAR(25),
    model VARCHAR(25),
    range INT,
    MSRP INT,
    CAFV_eligibility VARCHAR(25),
    Prev_CAFV VARCHAR(25),
    transac_load DATETIME,
    transac_end DATETIME,
    valid_startdate DATE,
    valid_end_date DATE,
    -- Foreign Key Constraints
    FOREIGN KEY (dim_date_id) REFERENCES dim_date(dim_date_id),
    FOREIGN KEY (dim_county_id) REFERENCES dim_county(dim_county_id),
    FOREIGN KEY (dim_state_id) REFERENCES dim_state(dim_state_id)
);



--population_fact
-- Create fact_population table
CREATE TABLE fact_population
(
    dim_pop_id INT IDENTITY(1,1) PRIMARY KEY,
    dim_year_id INT,
    dim_county_id INT,
    dim_jurisdiction_id INT,
    year INT,
    total_pop INT,
    eff_startdate DATE,
    eff_enddate DATE,
    -- Foreign Key Constraints
    FOREIGN KEY (dim_year_id) REFERENCES dim_year_lookup(dim_year_id),
    FOREIGN KEY (dim_county_id) REFERENCES dim_county(dim_county_id),
    FOREIGN KEY (dim_jurisdiction_id) REFERENCES dim_jurisdiction(dim_jurisdiction_id)
);


