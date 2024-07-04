-- data cleaning
use playoffs;
select * from layoffs;

create table playoffs_staging
like layoffs;

insert into playoffs_staging
select * from layoffs;

select * from playoffs_staging;

-- remove duplicates, create row number

select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions)
from playoffs_staging;


with dup as(
select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num
from playoffs_staging)
select * from dup
where row_num>1;

with dup as(
select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num
from playoffs_staging)
select * from dup
where company='Casper';

CREATE TABLE `playoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from playoffs_staging2 where row_num>1;

insert into playoffs_staging2
select *,row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num
from playoffs_staging;

delete from playoffs_staging2
where row_num > 1;

select * from playoffs_staging2 where company like"Clear%";

Update playoffs_staging2 set company='Clearco'
where company ='ClearCo';

select company, TRIM(company) 
from playoffs_staging2; 

Update playoffs_staging2 set company=
trim(company);
select distinct(industry) from playoffs_staging order by 1;

select * from playoffs_staging2 where industry like"Crypto%";

update playoffs_staging2 set industry='Crypto'
where industry like 'Crypto%';

select * from playoffs_staging order by industry;

-- update playoffs_staging set industry='unknown' or industry = null where industry='' or industry = null; 

select * from playoffs_staging order by industry;

update playoffs_staging2 set `date` =
str_to_date(`date`,'%m/%d/%Y');

alter table playoffs_staging2 modify column
`date` DATE; 
select distinct(`date`) from playoffs_staging2;

select distinct(country) from playoffs_staging order by 1;

select * from playoffs_staging where country like'United States%';

update playoffs_staging2 set country=trim(trailing '.' from country)
where country like 'United States%';
select * from playoffs_staging2 where country like'United States%';

select * from playoffs_staging2 where industry is null; -- or industry='';

update playoffs_staging2 set industry = null where industry='';

select t1.industry,t2.industry
from playoffs_staging2 t1
join playoffs_staging2 t2
where t1.company=t2.company
and t1.industry is null
and t2.industry is not null;

update playoffs_staging2 t1
join playoffs_staging2 t2
set
t1.industry= t2.industry
where t1.company=t2.company
and t1.industry is null
and t2.industry is not null;
select * from playoffs_staging2 where industry is null; -- or industry='';

delete from playoffs_staging2 where 
total_laid_off is null and percentage_laid_off is null;
select * from playoffs_staging2 where 
total_laid_off is null and percentage_laid_off is null;

alter table playoffs_staging2 drop column row_num;

select * from playoffs_staging; 