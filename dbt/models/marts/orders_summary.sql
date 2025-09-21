with src as (
  select * from {{ ref('stg_orders') }}
)
select
  order_status,
  count(*)::int as cnt
from src
group by order_status
order by order_status
