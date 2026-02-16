show warehouses;
show users;

show databases;

CREATE DATABASE production;

CREATE SCHEMA IF NOT EXISTS production.careers;

CREATE TABLE IF NOT EXISTS careers.job_listings (
  job_id NUMBER,
  job_title VARCHAR(500),
  company VARCHAR(500),
  descriptions VARCHAR(16777216),
  location VARCHAR(500),
  category VARCHAR(500),
  subcategory VARCHAR(500),
  role VARCHAR(200),
  type VARCHAR(100),
  salary VARCHAR(200),
  listing_date VARCHAR(50),
  salary_median_usd FLOAT
);

create role astronomer_rw;
grant role astronomer_rw to user danielafdzesp;

GRANT USAGE ON DATABASE production TO ROLE astronomer_rw;
GRANT USAGE ON SCHEMA careers TO ROLE astronomer_rw;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE PRODUCTION.careers.job_listings TO ROLE astronomer_rw;

show grants on schema careers;


show grants on database PRODUCTION;
show grants on schema PRODUCTION.careers;
show grants on table PRODUCTION.careers.job_listings;

DESCRIBE TABLE PRODUCTION.careers.job_listings;

ALTER TABLE PRODUCTION.careers.job_listings RENAME COLUMN role TO job_role;

-- Top 10 most demanded jobs
SELECT
TOP 10
    LOWER(job_title) AS job_title, 
    COUNT(*) AS total_titles
FROM
    PRODUCTION.careers.job_listings
GROUP BY
    ALL
ORDER BY
    total_titles DESC
;

-- Top 10 employers -> private advertiser
SELECT
TOP 10
    LOWER(company) AS company, 
    count(*) total_job_listings
FROM
    PRODUCTION.careers.job_listings
GROUP BY
    ALL
ORDER BY
    total_job_listings DESC
;


SELECT
    TYPE, 
    count(*)
FROM
    PRODUCTION.careers.job_listings
GROUP BY
    all
ORDER BY
    count(*) DESC
;