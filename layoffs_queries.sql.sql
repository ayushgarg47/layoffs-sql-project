# layoffs project: queries file (run AFTER layoffs_data.sql)
# Purpose: create backup, cleaning steps, creat final cleaned table
# and run analysis queries.
# How to run:
#1) Run layoffs_data.sql to load the raw table into DB.
#2) run this file (layoffs_queries.sql)

select count(*) as total_raw
from layoffs;
select * from layoffs;

-- create backup of raw table --
create table layoff_backup like layoffs;

insert into layoff_backup
select * from layoffs;


-- cleaning data --
-- finding and removing duplicates --
with cte_rn as 
(select *, row_number() over(partition by company, location, industry,
total_laid_off, percentage_laid_off, `date`, stage, country, 
funds_raised_millions) as rn 
from layoff_backup) select * from cte_rn
where rn > 1;

create table layoffs2
like layoff_backup;
alter table layoffs2
add rn int;

insert into layoffs2
select *, row_number() over(partition by company, location, industry,
total_laid_off, percentage_laid_off, `date`, stage, country, 
funds_raised_millions) as rn 
from layoff_backup;

select * from layoffs2
where rn > 1;

delete from layoffs2
where rn > 1;

-- standardization of data --
select * from layoffs2;
select trim(company), company
from layoffs2;
update layoffs2
set company = trim(company);

select distinct location from layoffs2;
select distinct industry from layoffs2;

update layoffs2 set industry = 'Crypto'
where industry like 'crypto%' ;

select distinct country from layoffs2;
update layoffs2 set country = 'United States'
where country like 'United States%' ;

-- standardizing date --
select `date` from layoffs2;
select `date`, str_to_date(`date`, '%m/%d/%Y') from layoffs2;
update layoffs2 
set `date` =  str_to_date(`date`, '%m/%d/%Y');

alter table layoffs2
modify column `date` date;

select*from layoffs2;
select * from layoffs2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs2;


-- deleting nulls --
delete from layoffs2
where total_laid_off is null
and percentage_laid_off is null;

alter table  layoffs2
drop column rn;

select * from layoffs2;

-- data analysis --
select * from layoffs2;

# layoffs by year
select year(`date`) as 'year', sum(total_laid_off) as total_layoffs
from layoffs2
where year(`date`) is not null
group by year(`date`)
order by 1 asc;

# layoffs by months
select year(`date`) as 'year', month(`date`) as 'month' ,
sum(total_laid_off) as total_layoffs
from layoffs2
where year(`date`) and month(`date`) is not null
group by year(`date`), month(`date`)
order by 1 asc;

# total layoffs by each country
select country, sum(total_laid_off)
from layoffs2
group by country
having sum(total_laid_off) is not null
order by 2 desc;

# top company with highest layoffs years wise
with cte_1 as 
(select company, year(`date`) as years, sum(total_laid_off) 
as total_off
from layoffs2
group by company, year(`date`)) 
, cte_2 as (
select*, dense_rank () 
over (partition by years order by total_off desc) as ranking
 from cte_1
 where years is not null
 and total_off is not null)
select * from cte_2
where ranking = 1;

-----------------












