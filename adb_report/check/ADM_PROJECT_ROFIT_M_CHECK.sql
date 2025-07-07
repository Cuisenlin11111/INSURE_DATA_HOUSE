-- @description: 项目利润数据校验
-- 定义临时表t，用于存储各项指标及计算录入成本比
WITH t AS (
    SELECT
        SUM(Case_Income) AS 案件收入,               -- 案件总收入
        SUM(Net_Profit) AS 净利润,                  -- 总净利润
        SUM(CLAIM_NUM) AS 案件量,                   -- 总案件数量
        SUM(SHLR_COST) AS 施博录入成本,             -- 施博录入总成本
        SUM(CDSJLR_COST) AS 成都视觉录入成本,       -- 成都视觉录入总成本
        SUM(GNLR_COST) AS 广纳录入成本,             -- 广纳录入总成本
        -- 计算录入成本比（总录入成本 / 案件总收入）
        (SUM(GNLR_COST) + SUM(CDSJLR_COST) + SUM(SHLR_COST)) / SUM(Case_Income) AS 录入成本比
    FROM claim_dm.ADM_PROJECT_ROFIT_M
    WHERE DT_MONTH = SUBSTRING('{formatted_date}', 1, 7)  -- 筛选指定月份的记录
)

-- 从临时表t中选择录入成本比超出指定范围的记录
SELECT *
FROM t
WHERE 录入成本比 > 0.35 OR 录入成本比 < 0.25;  -- 筛选录入成本比过高或过低的记录