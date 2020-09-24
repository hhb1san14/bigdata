-- 每个id浏览时长、步长
select id,
    (max(UNIX_TIMESTAMP(dt,'yyyy/MM/dd HH:mm')) - min(UNIX_TIMESTAMP(dt,'yyyy/MM/dd HH:mm')))/60 as defftime,
    count(1) as total
from t3 group by id;



-- 如果两次浏览之间的间隔超过30分钟，认为是两个不同的浏览时间；再求每个id浏览时长、步长
with temp as (
    select id,dt, case when d >= 30 then 1 else 0 end as gid
    from (
        select id,dt,
            (UNIX_TIMESTAMP(dt,'yyyy/MM/dd HH:mm') - UNIX_TIMESTAMP(lag(dt) over(partition by id order by dt) ,'yyyy/MM/dd HH:mm')) / 60 as d
        from t3
    ) t
)
select id,
    (max(UNIX_TIMESTAMP(dt,'yyyy/MM/dd HH:mm')) - min(UNIX_TIMESTAMP(dt,'yyyy/MM/dd HH:mm')))/60 as defftime,
    count(1) as total
from (
    select id,dt,
        sum(gid)over(partition by id order by dt) gid
    from temp
    ) t group by id,gid;