-- @description: 员工产能数据校验

-- 其他注释...
WITH t AS (
    SELECT
        name,
        work_hours,
        update_time,
        work_hours - LEAD(work_hours) OVER (PARTITION BY name ORDER BY update_time DESC) AS work_hours_diff
    FROM
        claim_dws.dws_employee_pro_df_bak
    WHERE
        dt = replace('{formatted_date}','-','')
)
SELECT * FROM t WHERE work_hours_diff < -0.5;