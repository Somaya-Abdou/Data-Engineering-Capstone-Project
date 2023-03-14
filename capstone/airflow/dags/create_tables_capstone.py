Create_all_tables = """

create table if not exists data.staging_sas_data (
cicid float ,
i94cit float,
i94res float,
arrdate date,
i94mode float,
i94addr text,
depdate float,
i94visa float,
biryear float,
gender text   );   

create table if not exists data.staging_country (
i94cntyl float ,
country text) ;
    
create table if not exists data.staging_visa (
i94visa float ,
visa_value text) ;
    
Create table if not exists data.staging_travel (
i94mode float,
travel_mode text) ;
    
create table if not exists data.staging_address(
i94addr text,
place text) ;

create table if not exists data.immigrants (
citizen_id float Primary Key,
country_cit text,
birth_year float,
gender text) ;
    
create table if not exists data.address (
citizen_id float Primary Key,
address text,
visa_type text ,
travel_mode text) ;
    
create table if not exists data.date (
citizen_id float Primary Key ,
arrival_date date,
arrival_day int,
arrival_month int,
arrival_year int ) ; """

                               
