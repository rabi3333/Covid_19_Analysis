/* first table "covid_19_data" is collected from 01/22/2020 to 05/25/2021
contains confirmed, deaths and recovered covid cases */

/* Second table "covid_vaccinations" is collected from 12/01/2020 to 02/01/2022
contains worldwide vaccination updates */

/* Third table "vaccines" is collected from 01/2021 to 02/2022 contains
total vaccinations for each manufacturer by country */

-- Importing first table

drop table if exists covid_19_data;
CREATE TABLE covid_19_data(
SNo INTEGER not null,
ObservationDate VARCHAR(255),
Province VARCHAR(255),
Country VARCHAR(255),
LastUpdate VARCHAR(255),
Confirmed DOUBLE,
Deaths DOUBLE,
Recovered DOUBLE
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/covid_19_data.csv'
INTO TABLE covid_19_data
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(SNo,ObservationDate,Province,Country,LastUpdate,Confirmed,Deaths,Recovered);


-- changing ObservationDate field type to date
update covid_19_data set ObservationDate = str_to_date(ObservationDate, "%m/%d/%Y");

/* Some values in Confirmed, Deaths and Recovered fields have a negative sign
by mistake */

update covid_19_data set Confirmed = abs(Confirmed), Deaths = abs(Deaths),
Recovered = abs(Recovered) ;

-- Importing second table

drop table if exists covid_vaccinations;
create table covid_vaccinations (
country varchar(255),
iso_code varchar(255),
observation_date date,
total_vaccinations double,
people_vaccinated double,
people_fully_vaccinated double,
daily_vaccinations_raw double,
daily_vaccinations double,
total_vaccinations_per_hundred double,
people_vaccinated_per_hundred double,
people_fully_vaccinated_per_hundred double,
daily_vaccinations_per_million double,
vaccines varchar(255),
source_name varchar(255),
source_website varchar(255)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/country_vaccinations.csv'
INTO TABLE covid_vaccinations
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(country,iso_code,observation_date,@vtotal_vaccinations,@vpeople_vaccinated,@vpeople_fully_vaccinated,@vdaily_vaccinations_raw,@vdaily_vaccinations,@vtotal_vaccinations_per_hundred,@vpeople_vaccinated_per_hundred,@vpeople_fully_vaccinated_per_hundred,@vdaily_vaccinations_per_million,@vvaccines,@vsource_name,@vsource_website
)
set
total_vaccinations = nullif(@vtotal_vaccinations, ''),
people_vaccinated = nullif(@vpeople_vaccinated, ''),
people_fully_vaccinated = nullif(@vpeople_fully_vaccinated, ''),
daily_vaccinations_raw = nullif(@vdaily_vaccinations_raw, ''),
daily_vaccinations= nullif(@vdaily_vaccinations, ''),
total_vaccinations_per_hundred= nullif(@vtotal_vaccinations_per_hundred, ''),
people_vaccinated_per_hundred= nullif(@vpeople_vaccinated_per_hundred, ''),
people_fully_vaccinated_per_hundred= nullif(@vpeople_fully_vaccinated_per_hundred, ''),
daily_vaccinations_per_million= nullif(@vdaily_vaccinations_per_million, '') ;

-- Importing third table

drop table if exists vaccines ;
create table vaccines (
location varchar(255),
`date` date ,
vaccine varchar(255),
total_vaccinations integer ) ;

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/country_vaccinations_by_manufacturer.csv'
INTO TABLE vaccines
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(location,`date`,vaccine,total_vaccinations) ;

-- Total cases vs Total death vs Total recovered by country

select Country, sum(Confirmed) as total_cases, sum(Deaths) as total_deaths,
sum(Recovered) as total_recovered
from covid_19_data
group by Country
order by Country ;

-- Death percentage vs recovered percentage

with cte1 as (
select Country, sum(Confirmed) as total_cases, sum(Deaths) as total_deaths,
sum(Recovered) as total_recovered
from covid_19_data
group by Country
order by Country ) 
select *, (total_deaths/total_cases)*100 as death_percentage,
(total_recovered/total_cases)*100 as recovered_percentage
from cte1
order by total_cases desc ;

-- worldwide total cases vs total deaths vs total recovered for each day

select ObservationDate, sum(Confirmed) as total_cases, sum(Deaths) as total_deaths,
sum(Recovered) as total_recovered
from covid_19_data
group by ObservationDate
order by ObservationDate ;

/* total cases vs total deaths vs total recovered for each day for every
different country */

with cte2 as(
select ObservationDate, Country,
sum(Confirmed) as confirmed_cases
from covid_19_data
group by Country, ObservationDate)
select ObservationDate, Country,
sum(confirmed_cases) over (partition by Country order by ObservationDate,
Country) as total_cases
from cte2 ;

-- looking at which provinces have the most cases

select Province, Country,
sum(Confirmed) as total_cases
from covid_19_data
where Province != ""
group by Province
Order by total_cases desc ;

/* total vaccinations vs total people vaccinated vs total people fully
vaccinated by country */

select country, max(total_vaccinations) as total_vac,
max(people_vaccinated) as total_people_vac,
max(people_fully_vaccinated) as total_people_fully_vac
from covid_vaccinations
group by country
order by total_vac desc ;

-- people vaccinated by the end of each month worldwide

with cte3 as (
select str_to_date(concat(month(observation_date),"-", year(observation_date)), "%m-%Y") as
month_of_year, country, max(people_vaccinated) as people_vaccinated_by_end_of_month_country
from covid_vaccinations
group by month_of_year, country
order by month_of_year, country)
select month_of_year, sum(people_vaccinated_by_end_of_month_country) as
people_vaccinated_by_end_of_month_worldwide
from cte3
group by month_of_year
order by month_of_year;

-- looking for total vaccinations for each manufacturer by country

select location,vaccine, sum(total_vaccinations) as total
from vaccines
group by location, vaccine
order by location, vaccine;

-- looking for which vaccine is most available 

select vaccine, sum(total_vaccinations) as total 
from vaccines
where location != "European Union"
group by vaccine
order by total desc;

-- looking for which vaccine is most used for each country

select T1.location, vaccine, T2.total
from (select location,vaccine, sum(total_vaccinations) as total
from vaccines
group by location, vaccine
order by location, vaccine) as T1 inner join(
with cte4 as (
select location,vaccine, sum(total_vaccinations) as Total
from vaccines
group by location, vaccine
order by location, vaccine)
select location, max(Total) as total
from cte4
group by location
order by location) as T2
on T1.location=T2.location and T1.Total=T2.total;