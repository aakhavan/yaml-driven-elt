select
  cast(order_id as integer) as order_id,
  order_status,
  cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp
from {{ ref('orders') }}
