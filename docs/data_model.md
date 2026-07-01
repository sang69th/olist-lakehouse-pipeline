# Gold Data Model — Star Schema

Central fact table surrounded by dimensions. This is what the pipeline builds.

## fact_orders  (the measurable events)
Grain: one row per order item (order_id + order_item_id).

| Column | From raw Olist | Meaning |
|---|---|---|
| order_id | orders | which order |
| order_item_id | order_items | line item number |
| customer_key | orders.customer_id | link to dim_customer |
| product_key | order_items.product_id | link to dim_product |
| seller_key | order_items.seller_id | link to dim_seller |
| order_date_key | orders.order_purchase_timestamp | link to dim_date |
| price | order_items.price | item price (measure) |
| freight_value | order_items.freight_value | shipping cost (measure) |
| delivery_days | delivered - purchase timestamp | how long delivery took (measure) |
| review_score | order_reviews.review_score | 1-5 rating (measure) |

## dim_customer  (who) — tracks history via SCD2
customer_id, customer_unique_id, city, state

## dim_product  (what)
product_id, category_english, weight_g, length/height/width_cm

## dim_seller  (who sold)
seller_id, city, state

## dim_date  (when)
date_key, full_date, year, month, day, weekday
