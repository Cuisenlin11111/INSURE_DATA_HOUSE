
-- 创建临时表current_data，存储当前日期符合条件的数据
WITH current_data AS (
    SELECT INSURE_COMPANY_CHANNEL, ZDSHAL_RW
    FROM CLAIM_DWD.DWD_CLAIM_COUNT_DAY
    WHERE GMT_CREATED = CURDATE()
      AND INSURE_COMPANY_CHANNEL NOT IN (
        '平安产险_雇主',
        '平安产险_团',
        '平安产险_个',
        '平安产险_车险',
        '众安暖哇_团',
        '众安暖哇_个',
          '暖哇科技','太保产险宁波分公司'
    )
),
-- 创建临时表last_week_data，存储上一周符合条件的数据，并计算平均ZDSHAL_RW
last_week_data AS (
    SELECT INSURE_COMPANY_CHANNEL, AVG(ZDSHAL_RW) AS avg_ZDSHAL_RW_last_week
    FROM (
        SELECT INSURE_COMPANY_CHANNEL, ZDSHAL_RW
        FROM CLAIM_DWD.DWD_CLAIM_COUNT_DAY
        WHERE DATE(GMT_CREATED) >= DATE_SUB(
            DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY),
            INTERVAL 7 DAY
        )
        AND DATE(GMT_CREATED) <= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY)
        AND INSURE_COMPANY_CHANNEL NOT IN (
            '平安产险_雇主',
            '平安产险_团',
            '平安产险_个',
            '平安产险_车险',
            '众安暖哇_团',
            '众安暖哇_个',
          '暖哇科技','太保产险宁波分公司'
        )
    ) AS subquery
    GROUP BY INSURE_COMPANY_CHANNEL
)
-- 连接两个临时表，计算波动并筛选出波动超过50%的渠道
SELECT
    cd.INSURE_COMPANY_CHANNEL
FROM current_data cd
JOIN last_week_data lw ON cd.INSURE_COMPANY_CHANNEL = lw.INSURE_COMPANY_CHANNEL
WHERE
    ((cd.ZDSHAL_RW - lw.avg_ZDSHAL_RW_last_week) / lw.avg_ZDSHAL_RW_last_week) * 100 > 50
    OR ((cd.ZDSHAL_RW - lw.avg_ZDSHAL_RW_last_week) / lw.avg_ZDSHAL_RW_last_week) * 100 < -50
;




