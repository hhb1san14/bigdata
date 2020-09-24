
with temp as (
select
    id,time,price,
    lag(price) over(partition by id order by time) as lagprice,
    lead(price) over(partition by id order by time) as leadprice
 from t2)
 select
    id,time,price,
    case when price >= lagprice and price >=leadprice then '波峰'
         when price <= lagprice and price <=leadprice then '波谷'
         else '未知' end as   feature
  from temp
  where  case when price >= lagprice and price >=leadprice then '波峰'
         when price <= lagprice and price <=leadprice then '波谷'
         else '未知' end  in('波峰','波谷');