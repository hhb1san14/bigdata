



with  temp as  (select
    team,year,
    row_number()over(partition by team order by year) as rownum,
    (year - row_number()over(partition by team order by year)) as gid
 from t1)
 select * from (
 select team from temp group by team,gid having count(1) >= 3)t group by team;

