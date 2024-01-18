/*
    Try changing "table" to "view" below
*/

-- {{ config(materialized='table') }}

with weekly_ex_data as (

    SELECT země,
       měna,
       množství,
       kód,
       AVG(kurz) exchange_rate,
       TRUNC(DATE_TRUNC('WEEK',TO_DATE(exchange_date,'dd.mm.yyyy'))::TIMESTAMP) AS week
    FROM public.exchange_rates
    GROUP BY země,
         měna,
         množství,
         kód,
         week

)

select *
from weekly_ex_data

