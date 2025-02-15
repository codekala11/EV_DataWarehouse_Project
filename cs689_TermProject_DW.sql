USE CS689_PROJECT;

--Dim Staging Area
--county_dim_staging

MERGE INTO dim_county_staging AS target
USING (
    SELECT county, ROW_NUMBER() OVER (ORDER BY county) AS county_id
    FROM pop_df
    GROUP BY county
) AS source (county, county_id)
ON target.county_id = source.county_id
WHEN MATCHED AND target.county <> source.county THEN
    UPDATE SET
        target.prev_county_name = target.county,
        target.county = source.county
WHEN NOT MATCHED BY TARGET THEN
    INSERT (county_id, county)
    VALUES (source.county_id, source.county);


Select * from dim_county_staging;


--dim_date_staging

MERGE INTO dim_date_staging AS target
USING (
    SELECT 
        CAST(date AS DATE) AS full_date,
        ROW_NUMBER() OVER (ORDER BY CAST(date AS DATE)) AS date_id, 
        YEAR(date) AS year,
        MONTH(date) AS month,
        DAY(date) AS day,
        CASE 
            WHEN MONTH(date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(date) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END AS season
    FROM ev_pop
    GROUP BY CAST(date AS DATE), YEAR(date), MONTH(date), DAY(date),
        CASE 
            WHEN MONTH(date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(date) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END
) AS source (full_date, date_id, year, month, day, season)
ON target.full_date = source.full_date
WHEN MATCHED THEN
    UPDATE SET
        target.year = source.year,
        target.month = source.month,
        target.day = source.day,
        target.season = source.season
WHEN NOT MATCHED BY TARGET THEN
    INSERT (date_id, full_date, year, month, day, season, eff_startdate, eff_enddate, current_flag)
    VALUES (
        source.date_id,
        source.full_date,
        source.year,
        source.month,
        source.day,
        source.season,
        GETDATE(), 
        NULL,      
        1          
    );

SELECT * FROM dim_date_staging;


--dim_state_staging
INSERT INTO dim_state_staging(state_id, state, state_abbvr) VALUES (1, 'Washington', 'WA');

SELECT * FROM dim_state_staging;


--dim_vehicle_type_staging
INSERT INTO dim_vehicle_type_staging(vehicle_type_id, vehicle_type) VALUES
(1, 'passanger'),
(2, 'truck');

select * from dim_vehicle_type_staging;


--dim_jurisdiction_staging
INSERT INTO dim_jurisdiction_staging(jurisdiction_id, jurisdiction) VALUES
(1, 'Unincorporated'),
(2, 'Incorporated');

SELECT * FROM dim_jurisdiction_staging;


--dim_year_lookup_staging
INSERT INTO dim_year_lookup_staging (year_id, year)
VALUES
(1, 2017),
(2, 2018),
(3, 2019),
(4, 2020),
(5, 2021),
(6, 2022),
(7, 2023),
(8, 2024);


INSERT INTO dim_year_lookup_staging (year_id, year)
SELECT 
    max_id + 1 AS year_id,
    max_year + 1 AS year
FROM (
    SELECT 
        MAX(year_id) AS max_id,
        MAX(year) AS max_year
    FROM dim_year_lookup_staging
) AS subquery
WHERE NOT EXISTS (
    SELECT 1
    FROM dim_year_lookup_staging
    WHERE year = (SELECT MAX(year) + 1 FROM dim_year_lookup_staging)
);

SELECT * FROM dim_year_lookup_staging;


--fact staging tables

--fact_ev_usage_staging
INSERT INTO fact_ev_usage_staging (
    date_id,
    county_id,
    state_id,
    vehicle_type_id,
    PHEV_count,
    BEV_count,
    EV_count
)
SELECT
    d.date_id,                         
    c.county_id,                       
    s.state_id,                        
    vt.vehicle_type_id,                
    e.PHEV AS PHEV_count,                  
    e.BEV AS BEV_count,                    
    e.EV AS EV_count                       
FROM ev_pop e
JOIN dim_date_staging d 
    ON e.date = d.full_date               
JOIN dim_county_staging c 
    ON e.county = c.county                 
JOIN dim_state_staging s 
    ON e.state = s.state_abbvr             
JOIN dim_vehicle_type_staging vt 
    ON e.primary_use = vt.vehicle_type; 

select * from fact_ev_usage_staging;


--fact-pop_ev_staging
INSERT INTO fact_pop_ev_staging (
    date_id,
    county_id,
    state_id,
    jurisdiction_id,
    PHEV_count,
    BEV_count,
    EV_count,
    Non_ev_count,
    total_count,
    percent_ev
)
SELECT
    d.date_id,                           
    c.county_id,                         
    s.state_id,                       
    j.jurisdiction_id,                
    e.PHEV AS PHEV_count,                 
    e.BEV AS BEV_count,                    
    e.EV AS EV_count,                       
    e.Non_EV AS Non_ev_count,             
    e.Total_vehicles AS total_count,       
    e.percent_EV AS percent_ev              
FROM ev_pop e
JOIN dim_date_staging d 
    ON e.date = d.full_date                  
JOIN dim_county_staging c 
    ON e.county = c.county                   
JOIN dim_state_staging s 
    ON e.state = s.state_abbvr              
JOIN pop_df p 
    ON e.county = p.county                   
JOIN dim_jurisdiction_staging j 
    ON p.jurisdiction = j.jurisdiction;      
        
SELECT * FROM fact_pop_ev_staging;


--vehicle_detail_fact
INSERT INTO fact_vehicle_detail_staging (
    VIN,
    date_id,
    county_id,
    state_id,
    model_year,
    maker,
    model,
    range,
    MSRP,
    CAFV_eligibility,
    Prev_CAFV,
    valid_startdate,
    valid_end_date
)
SELECT
    e.VIN,
    d.date_id,
    c.county_id,
    s.state_id,
    e.model_year,
    e.make AS maker,
    e.model,
    e.range,
    e.MSRP,
    e.CAFV_eligibility,
    NULL AS Prev_CAFV,
    GETDATE() AS valid_startdate,
    NULL AS valid_end_date
FROM ev_df e
JOIN dim_date_staging d 
    ON e.model_year = d.year
JOIN dim_county_staging c 
    ON e.county = c.county
JOIN dim_state_staging s 
    ON e.state = s.state_abbvr;
            

SELECT * FROM fact_vehicle_detail_staging;



--fact_population_staging
INSERT INTO fact_population_staging (
    pop_id,
    year_id,
    county_id,
    jurisdiction_id,
    year,
    total_pop,
    eff_startdate,
    eff_enddate
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY p.sequence) AS dim_pop_id,
    y.year_id,
    c.county_id,
    j.jurisdiction_id,
    y.year,
    CASE
        WHEN y.year = 2017 THEN p.pop_2017
        WHEN y.year = 2018 THEN p.pop_2018
        WHEN y.year = 2019 THEN p.pop_2019
        WHEN y.year = 2020 THEN p.pop_2020
        WHEN y.year = 2021 THEN p.pop_2021
        WHEN y.year = 2022 THEN p.pop_2022
        WHEN y.year = 2023 THEN p.pop_2023
        WHEN y.year = 2024 THEN p.pop_2024
    END AS total_pop,
    GETDATE() AS eff_startdate,
    NULL AS eff_enddate
FROM pop_df p
JOIN dim_county_staging c ON p.county = c.county
JOIN dim_jurisdiction_staging j ON p.jurisdiction = j.jurisdiction
JOIN dim_year_lookup_staging y ON y.year BETWEEN 2017 AND 2024;


SELECT * FROM fact_population_staging;



--data warehouse
--dimension table

--dim_county
MERGE dim_county AS target
USING dim_county_staging AS source
ON target.county_id = source.county_id
WHEN MATCHED AND target.county <> source.county THEN
    UPDATE SET
        target.prev_county_name = target.county,  
        target.county = source.county             
WHEN NOT MATCHED BY TARGET THEN
    INSERT (county_id, county, prev_county_name)
    VALUES (source.county_id, source.county, NULL);  

select * from dim_county;

----------------------------------------------------------------------------------------------------------------------------
--test case
UPDATE dim_county_staging
SET county = 'Asoti'
WHERE county_id = 2;


SELECT * FROM dim_county WHERE county_id = 2;
-------------------------------------------------------------------------------------------------------------------------------------


--dim_date
MERGE INTO dim_date AS target
USING dim_date_staging AS source
ON target.full_date = source.full_date
   AND target.eff_enddate IS NULL 
WHEN MATCHED AND (
    target.year <> source.year OR
    target.month <> source.month OR
    target.day <> source.day OR
    target.season <> source.season
) THEN
    UPDATE SET 
        target.eff_enddate = GETDATE(),  
        target.current_flag = 0          
WHEN NOT MATCHED BY TARGET THEN
    INSERT (date_id, full_date, year, month, day, season, eff_startdate, eff_enddate, current_flag)
    VALUES (
        source.date_id,
        source.full_date,
        source.year,
        source.month,
        source.day,
        source.season,
        GETDATE(), 
        NULL,       
        1           
    );

select * from dim_date;


--dim_state
MERGE INTO dim_state AS target
USING dim_state_staging AS source
ON target.state_id = source.state_id  
WHEN MATCHED AND (                      
    target.state <> source.state OR
    target.state_abbvr <> source.state_abbvr
) THEN
    UPDATE SET 
        target.state = source.state,
        target.state_abbvr = source.state_abbvr
WHEN NOT MATCHED BY TARGET THEN
    INSERT (state_id, state, state_abbvr)
    VALUES (
        source.state_id,
        source.state,
        source.state_abbvr
    );

select * from dim_state;


--dim_vehicle_type
MERGE dim_vehicle_type AS target
USING dim_vehicle_type_staging AS source
ON target.vehicle_type_id = source.vehicle_type_id  
WHEN MATCHED AND target.vehicle_type <> source.vehicle_type THEN
    UPDATE SET
        target.vehicle_type = source.vehicle_type 
WHEN NOT MATCHED BY TARGET THEN
    INSERT (vehicle_type_id, vehicle_type)
    VALUES (source.vehicle_type_id, source.vehicle_type);

select * from dim_vehicle_type;


--dim_jurisdiction
MERGE dim_jurisdiction AS target
USING dim_jurisdiction_staging AS source
ON target.jurisdiction_id = source.jurisdiction_id 
WHEN MATCHED AND target.jurisdiction <> source.jurisdiction THEN
    UPDATE SET
        target.jurisdiction = source.jurisdiction 
WHEN NOT MATCHED BY TARGET THEN
    INSERT (jurisdiction_id, jurisdiction)
    VALUES (source.jurisdiction_id, source.jurisdiction);  

select * from dim_jurisdiction;


--dim_year_lookup
MERGE dim_year_lookup AS target
USING dim_year_lookup_staging AS source
ON target.year_id = source.year_id  
WHEN MATCHED AND target.year <> source.year THEN
    UPDATE SET
        target.year = source.year  
WHEN NOT MATCHED BY TARGET THEN
    INSERT (year_id, year)
    VALUES (source.year_id, source.year);  

select * from dim_year_lookup;


--fact tables

--fact-ev-usage
MERGE fact_ev_usage AS target
USING (
    SELECT
        source.date_id,
        source.county_id,
        source.state_id,
        source.vehicle_type_id,
        source.PHEV_count,
        source.BEV_count,
        source.EV_count,
        d.dim_date_id,
        c.dim_county_id,
        s.dim_state_id,
        v.dim_vehicle_type_id
    FROM fact_ev_usage_staging source
    JOIN dim_date d ON source.date_id = d.date_id
    JOIN dim_county c ON source.county_id = c.county_id
    JOIN dim_state s ON source.state_id = s.state_id
    JOIN dim_vehicle_type v ON source.vehicle_type_id = v.vehicle_type_id
) AS source
ON target.date_id = source.date_id
   AND target.county_id = source.county_id
   AND target.state_id = source.state_id
   AND target.vehicle_type_id = source.vehicle_type_id  
WHEN MATCHED AND (
    target.PHEV_count <> source.PHEV_count OR
    target.BEV_count <> source.BEV_count OR
    target.EV_count <> source.EV_count
) THEN
    UPDATE SET
        target.PHEV_count = source.PHEV_count,
        target.BEV_count = source.BEV_count,
        target.EV_count = source.EV_count
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        dim_date_id, dim_county_id, dim_state_id, dim_vehicle_type_id,
        date_id, county_id, state_id, vehicle_type_id,
        PHEV_count, BEV_count, EV_count
    )
    VALUES (
        source.dim_date_id, source.dim_county_id, source.dim_state_id, source.dim_vehicle_type_id,
        source.date_id, source.county_id, source.state_id, source.vehicle_type_id,
        source.PHEV_count, source.BEV_count, source.EV_count
    );


select * from fact_ev_usage;


--fact_pop_ev
MERGE fact_pop_ev AS target
USING (
    SELECT
        source.date_id,
        source.county_id,
        source.state_id,
        source.jurisdiction_id,
        source.PHEV_count,
        source.BEV_count,
        source.EV_count,
        source.Non_ev_count,
        source.total_count,
        source.percent_ev,
        d.dim_date_id,
        c.dim_county_id,
        s.dim_state_id,
        j.dim_jurisdiction_id
    FROM fact_pop_ev_staging source
    JOIN dim_date d ON source.date_id = d.date_id
    JOIN dim_county c ON source.county_id = c.county_id
    JOIN dim_state s ON source.state_id = s.state_id
    JOIN dim_jurisdiction j ON source.jurisdiction_id = j.jurisdiction_id
) AS source
ON target.dim_date_id = source.dim_date_id
   AND target.dim_county_id = source.dim_county_id
   AND target.dim_state_id = source.dim_state_id
   AND target.dim_jurisdiction_id = source.dim_jurisdiction_id
WHEN MATCHED AND (
    target.PHEV_count <> source.PHEV_count OR
    target.BEV_count <> source.BEV_count OR
    target.EV_count <> source.EV_count OR
    target.Non_ev_count <> source.Non_ev_count OR
    target.total_count <> source.total_count OR
    target.percent_ev <> source.percent_ev
) THEN
    UPDATE SET
        target.PHEV_count = source.PHEV_count,
        target.BEV_count = source.BEV_count,
        target.EV_count = source.EV_count,
        target.Non_ev_count = source.Non_ev_count,
        target.total_count = source.total_count,
        target.percent_ev = source.percent_ev
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        dim_date_id, dim_county_id, dim_state_id, dim_jurisdiction_id,
        PHEV_count, BEV_count, EV_count, Non_ev_count, total_count, percent_ev
    )
    VALUES (
        source.dim_date_id, source.dim_county_id, source.dim_state_id, source.dim_jurisdiction_id,
        source.PHEV_count, source.BEV_count, source.EV_count, source.Non_ev_count, source.total_count, source.percent_ev
    );


select * from fact_pop_ev;


--fact_vehicle_detail
MERGE fact_vehicle_detail AS target
USING (
    SELECT
        source.VIN,
        source.model_year,
        source.maker,
        source.model,
        source.range,
        source.MSRP,
        source.CAFV_eligibility,
        d.dim_date_id,
        c.dim_county_id,
        s.dim_state_id,
        GETDATE() AS current_load_date
    FROM fact_vehicle_detail_staging source
    JOIN dim_date d ON source.date_id = d.date_id
    JOIN dim_county c ON source.county_id = c.county_id
    JOIN dim_state s ON source.state_id = s.state_id
) AS source
ON target.VIN = source.VIN
   AND target.valid_end_date IS NULL 
WHEN MATCHED AND (
    target.dim_date_id <> source.dim_date_id OR
    target.dim_county_id <> source.dim_county_id OR
    target.dim_state_id <> source.dim_state_id OR
    target.model_year <> source.model_year OR
    target.maker <> source.maker OR
    target.model <> source.model OR
    target.range <> source.range OR
    target.MSRP <> source.MSRP OR
    target.CAFV_eligibility <> source.CAFV_eligibility
) THEN
    UPDATE SET
        target.valid_end_date = GETDATE(),
        target.transac_end = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        VIN,
        dim_date_id,
        dim_county_id,
        dim_state_id,
        model_year,
        maker,
        model,
        range,
        MSRP,
        CAFV_eligibility,
        Prev_CAFV,
        transac_load,
        transac_end,
        valid_startdate,
        valid_end_date
    )
    VALUES (
        source.VIN,
        source.dim_date_id,
        source.dim_county_id,
        source.dim_state_id,
        source.model_year,
        source.maker,
        source.model,
        source.range,
        source.MSRP,
        source.CAFV_eligibility,
        NULL,                       
        source.current_load_date,  
        NULL,                       
        source.current_load_date,   
        NULL                        
    );


select * from fact_vehicle_detail; 


--fact_population
MERGE fact_population AS target
USING (
    SELECT
        y.dim_year_id,
        c.dim_county_id,
        j.dim_jurisdiction_id,
        s.year,
        s.total_pop,
        GETDATE() AS current_date_value 
    FROM fact_population_staging s
    JOIN dim_year_lookup y ON s.year = y.year
    JOIN dim_county c ON s.county_id = c.county_id
    JOIN dim_jurisdiction j ON s.jurisdiction_id = j.jurisdiction_id
) AS source
ON target.dim_year_id = source.dim_year_id
   AND target.dim_county_id = source.dim_county_id
   AND target.dim_jurisdiction_id = source.dim_jurisdiction_id
   AND target.year = source.year
WHEN MATCHED AND target.total_pop <> source.total_pop THEN
    UPDATE SET
        target.eff_enddate = GETDATE() 
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        dim_year_id,
        dim_county_id,
        dim_jurisdiction_id,
        year,
        total_pop,
        eff_startdate,
        eff_enddate
    )
    VALUES (
        source.dim_year_id,
        source.dim_county_id,
        source.dim_jurisdiction_id,
        source.year,
        source.total_pop,
        GETDATE(), 
        NULL 
    );


select * from fact_population;

-----------------------------------------------------------------------------------------------------------------------------------------
--test case to see if changes are getting tracked
UPDATE fact_population_staging
SET total_pop = total_pop + 500
WHERE year_id = 1 AND county_id = 1 AND jurisdiction_id = 2 AND year = 2017;


SELECT * FROM fact_population
WHERE dim_year_id = 1 AND dim_county_id = 1 AND dim_jurisdiction_id = 2 AND year = 2017;
------------------------------------------------------------------------------------------------------------------------------------------



TRUNCATE TABLE dim_county_staging;
TRUNCATE TABLE dim_date_staging;
TRUNCATE TABLE dim_state_staging;
TRUNCATE TABLE dim_vehicle_type_staging;
TRUNCATE TABLE dim_jurisdiction_staging;
TRUNCATE TABLE dim_year_lookup_staging;

TRUNCATE TABLE fact_ev_usage_staging;
TRUNCATE TABLE fact_pop_ev_staging;
TRUNCATE TABLE fact_vehicle_detail_staging;
TRUNCATE TABLE fact_population_staging;



--business question 1
SELECT TOP 1
    c.county AS county_name,
    SUM(f.EV_count) AS total_evs
FROM fact_ev_usage f
JOIN dim_county c ON f.dim_county_id = c.dim_county_id
JOIN dim_state s ON f.dim_state_id = s.dim_state_id
WHERE s.state = 'Washington' 
GROUP BY c.county
ORDER BY total_evs DESC;  


--business question 2
-- Most Bought Model
SELECT TOP 1
    f.model AS most_bought_model,
    COUNT(*) AS total_sold
FROM fact_vehicle_detail f
GROUP BY f.model
ORDER BY total_sold DESC;

-- Most Famous Maker
SELECT TOP 1
    f.maker AS most_famous_maker,
    COUNT(*) AS total_sold
FROM fact_vehicle_detail f
GROUP BY f.maker
ORDER BY total_sold DESC;

--business question3
-- was there a significant increase in purchase of Ev vehicles post the pandemic
WITH PrePandemicEVs AS (
    SELECT
        d.year,
        SUM(f.EV_count) AS total_pre_pandemic_evs
    FROM fact_ev_usage f
    JOIN dim_date d ON f.dim_date_id = d.dim_date_id
    WHERE d.year <= 2019  
    GROUP BY d.year
),
PostPandemicEVs AS (
    SELECT
        d.year,
        SUM(f.EV_count) AS total_post_pandemic_evs
    FROM fact_ev_usage f
    JOIN dim_date d ON f.dim_date_id = d.dim_date_id
    WHERE d.year >= 2021  
    GROUP BY d.year
)
SELECT
    'Pre-Pandemic' AS period,
    SUM(total_pre_pandemic_evs) AS total_evs
FROM PrePandemicEVs
UNION ALL
SELECT
    'Post-Pandemic' AS period,
    SUM(total_post_pandemic_evs) AS total_evs
FROM PostPandemicEVs;


