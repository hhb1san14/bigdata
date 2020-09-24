
with t as(
-- 第三步，通过对user_id分组，对点击时间进行排序，对diff_time进行累加（第一行到当前行）默认的,并取别名为gid，作为下次分组的依据
select
    user_id,click_time,sum(diff_time) over(partition by user_id order by click_time) gid
from (
    -- 第二步，计算当前行与上一行时间的时间差，如果大于30分钟，赋值为1，其他的为0，并取别名为 diff_time
    select
        user_id,click_time,case when (unix_timestamp(click_time) - unix_timestamp(lag_click_time))/60 > 30 then 1 else 0 end diff_time
    from (
-- 第一步，现将当前行的上一行数据移动到当前行
        select
            user_id,click_time,lag(click_time)over(partition by user_id order by click_time )as lag_click_time
        from user_clicklog
        )temp1
    )temp2
)
-- 第四步，进行排序，使用开窗函数，对user_id，gid进行分组，并根据click_time排序
select
    user_id,click_time,dense_rank()over(partition by user_id,gid order by click_time) as rank
from t;




















