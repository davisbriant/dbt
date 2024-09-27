{{
  config(
    materialized='table',
    unique_key = 'pkey',
    partition_by = ['date'],
  )
}}


with dims as (

select
adid as adid2,
type,
status,
finalurls
from
{{ ref('dim_googleads_performancereport_ads')}}
group by
1,2,3,4

),
facts as (

select 
*
from 
{{ ref('fact_googleads_performancereport_ads')}}

),

joined as (

select 
facts.*,
dims.*
from 
facts
LEFT JOIN 
dims
on facts.adid = dims.adid2
)

select 
* 
from 
TABLE(exclude_columns(input => table(joined),columns => DESCRIPTOR(adid2)))