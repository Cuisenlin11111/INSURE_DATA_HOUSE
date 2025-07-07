# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime

sql_query = f"""
  -- @description: 因朔桔成本报表（录入维度）
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2025-01-06 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO CLAIM_DWD.DWD_YSJ_LR_COST_STATISTICS (
    channel,
    data_dt,
    kt_num,
    kt_cost,
    ocr_num,
    ocr_cost,
    hsbx_y_num,
    hsbx_y_cost,
    hsbx_n_num,
    hsbx_n_cost,
    inp,
    inp_cost,
    rev,
    rev_cost,
    wsy_num,
    wsy_cost
)
SELECT
    -- 通过关联获取渠道名称
    cic.channel_value AS 渠道,
    -- 日期字段
    COALESCE(kt.data_dt, ocr.data_dt, hsbx_y.data_dt, hsbx_n.data_dt, inp.data_dt, rev.data_dt, wsy.data_dt) AS 日期,
    -- 快瞳电子票调用次数
    cast(COALESCE(kt.kt_num, 0) as int) AS 快瞳电子票调用次数,
    -- 快瞳电子票调用费用，根据单价与调用次数相乘计算，保留两位小数
    cast(COALESCE(kt.kt_num, 0) * 0.07 as DECIMAL(10, 2)) AS 快瞳电子票调用费用,
    -- ocr调用次数
    cast(COALESCE(ocr.ocr_num, 0) as int) AS ocr调用次数,
    -- ocr调用费用
    cast(COALESCE(ocr.ocr_num, 0) * 0.15 as DECIMAL(10, 2)) AS ocr调用费用,
    -- 海盛宝信电子票调（无明细）
    cast(COALESCE(hsbx_y.hsbx_y_num, 0) as int) AS 海盛宝信电子票调无明细,
    -- 海盛宝信电子票调（无明细）费用
    cast(COALESCE(hsbx_y.hsbx_y_num, 0) * 0.12 as DECIMAL(10, 2)) AS 海盛宝信电子票调无明细费用,
    -- 海盛宝信电子票(有明细)
    cast(COALESCE(hsbx_n.hsbx_n_num, 0) as int) AS 海盛宝信电子票有明细,
    -- 海盛宝信电子票(有明细)费用
    cast(COALESCE(hsbx_n.hsbx_n_num, 0) * 0.20 as DECIMAL(10, 2)) AS 海盛宝信电子票有明细费用,
    -- 录入次数
    cast(COALESCE(inp.inp_count, 0) as int) AS 录入,
    -- 录入费用
    cast(COALESCE(inp.inp_count, 0) * 0.45 as DECIMAL(10, 2)) AS 录入费用,
    -- 复核次数
    cast(COALESCE(rev.rev_count, 0) as int) AS 复核,
    -- 复核费用
    cast(COALESCE(rev.rev_count, 0) * 0.21 as DECIMAL(10, 2)) AS 复核费用,
    -- 五要素次数
    cast(COALESCE(wsy.wsy_num, 0) as int) AS 五要素,
    -- 五要素费用
    cast(COALESCE(wsy.wsy_num, 0) * 0.05 as DECIMAL(10, 2)) AS 五要素费用
FROM
    -- 快瞳电子票调用次数的子查询，作为主表进行左连接
    (
        SELECT
            substr(inphile_ods.algorithm_task.case_id, 1, 4) insure_company_channel,
            substr(inphile_ods.algorithm_task.create_time, 1, 10) data_dt,
            count(1) kt_num
        FROM
            inphile_ods.algorithm_task
        WHERE
            inphile_ods.algorithm_task.channel_type IN ('2', '3', '4', '7', '9', '12')
        GROUP BY
            substr(inphile_ods.algorithm_task.case_id, 1, 4), substr(inphile_ods.algorithm_task.create_time, 1, 10)
    ) AS kt
    -- 左连接ocr调用次数的子查询
    LEFT JOIN
    (
        SELECT
            inphile_ods.case_info.insure_company_channel,
            substr(inphile_ods.white_board_result.create_time, 1, 10) data_dt,
            count(1) ocr_num
        FROM
            inphile_ods.white_board_result
        LEFT JOIN
            inphile_ods.case_image ON inphile_ods.white_board_result.image_id = inphile_ods.case_image.id
        LEFT JOIN
            inphile_ods.case_info ON inphile_ods.case_image.case_id = inphile_ods.case_info.id
        WHERE
            inphile_ods.white_board_result.deleted = 0
        GROUP BY
            inphile_ods.case_info.insure_company_channel, substr(inphile_ods.white_board_result.create_time, 1, 10)
    ) AS ocr ON kt.insure_company_channel = ocr.insure_company_channel AND kt.data_dt = ocr.data_dt
    -- 左连接海盛宝信电子票（无明细）的子查询
    LEFT JOIN
    (
        SELECT
            substr(inphile_ods.algorithm_task.case_id, 1, 4) insure_company_channel,
            substr(inphile_ods.algorithm_task.create_time, 1, 10) data_dt,
            count(1) hsbx_y_num
        FROM
            inphile_ods.algorithm_task
        WHERE
            inphile_ods.algorithm_task.channel_type IN ('8', '13') AND inphile_ods.algorithm_task.has_detail = 2
        GROUP BY
            substr(inphile_ods.algorithm_task.case_id, 1, 4), substr(inphile_ods.algorithm_task.create_time, 1, 10)
    ) AS hsbx_y ON kt.insure_company_channel = hsbx_y.insure_company_channel AND kt.data_dt = hsbx_y.data_dt
    -- 左连接海盛宝信电子票（有明细）的子查询
    LEFT JOIN
    (
        SELECT
            substr(inphile_ods.algorithm_task.case_id, 1, 4) insure_company_channel,
            substr(inphile_ods.algorithm_task.create_time, 1, 10) data_dt,
            count(1) hsbx_n_num
        FROM
            inphile_ods.algorithm_task
        WHERE
            inphile_ods.algorithm_task.channel_type IN ('8', '13') AND inphile_ods.algorithm_task.has_detail = 1
        GROUP BY
            substr(inphile_ods.algorithm_task.case_id, 1, 4), substr(inphile_ods.algorithm_task.create_time, 1, 10)
    ) AS hsbx_n ON kt.insure_company_channel = hsbx_n.insure_company_channel AND kt.data_dt = hsbx_n.data_dt
    -- 左连接录入次数的子查询
    LEFT JOIN
    (
        SELECT
            substr(inphile_ods.case_info.id, 1, 4) as insure_company_channel,
            substr(inphile_ods.case_log.create_time, 1, 10) data_dt,
            COUNT(DISTINCT inphile_ods.case_bill.id) AS inp_count
        FROM
            inphile_ods.case_info
        LEFT JOIN
            inphile_ods.case_log ON inphile_ods.case_info.id = inphile_ods.case_log.case_id
        LEFT JOIN
            inphile_ods.account ON inphile_ods.case_log.user_id = inphile_ods.account.account
        LEFT JOIN
            inphile_ods.case_apply ON inphile_ods.case_apply.case_id = inphile_ods.case_info.id
        LEFT JOIN
            inphile_ods.case_bill ON inphile_ods.case_bill.apply_id = inphile_ods.case_apply.id
        WHERE
            inphile_ods.case_log.node = '录入' AND inphile_ods.case_log.handle = '账核诊录入'
            AND inphile_ods.case_info.deleted = 0
        GROUP BY
            substr(inphile_ods.case_info.id, 1, 4), substr(inphile_ods.case_log.create_time, 1, 10)
    ) AS inp ON kt.insure_company_channel = inp.insure_company_channel AND kt.data_dt = inp.data_dt
    -- 左连接复核次数的子查询
    LEFT JOIN
    (
        SELECT
            substr(inphile_ods.case_info.id, 1, 4) as insure_company_channel,
            substr(inphile_ods.case_log.create_time, 1, 10) data_dt,
            COUNT(DISTINCT inphile_ods.case_bill.id) AS rev_count
        FROM
            inphile_ods.case_info
        LEFT JOIN
            inphile_ods.case_log ON inphile_ods.case_info.id = inphile_ods.case_log.case_id
        LEFT JOIN
            inphile_ods.account ON inphile_ods.case_log.user_id = inphile_ods.account.account
        LEFT JOIN
            inphile_ods.case_apply ON inphile_ods.case_apply.case_id = inphile_ods.case_info.id
        LEFT JOIN
            inphile_ods.case_bill ON inphile_ods.case_bill.apply_id = inphile_ods.case_apply.id
        WHERE
            inphile_ods.case_log.node IN ('复核', '质检')
            AND inphile_ods.case_log.handle IN ('复核录入', '质检录入')
            AND inphile_ods.case_info.deleted = 0
        GROUP BY
            substr(inphile_ods.case_info.id, 1, 4), substr(inphile_ods.case_log.create_time, 1, 10)
    ) AS rev ON kt.insure_company_channel = rev.insure_company_channel AND kt.data_dt = rev.data_dt
    -- 左连接五要素次数的子查询
    LEFT JOIN
    (
        SELECT
            substr(inphile_ods.algorithm_task.case_id, 1, 4) insure_company_channel,
            substr(inphile_ods.algorithm_task.create_time, 1, 10) data_dt,
            count(1) wsy_num
        FROM
            inphile_ods.algorithm_task
        WHERE
            inphile_ods.algorithm_task.channel_type IN ('12', '13')
            AND inphile_ods.algorithm_task.task_type = 10
        GROUP BY
            substr(inphile_ods.algorithm_task.case_id, 1, 4), substr(inphile_ods.algorithm_task.create_time, 1, 10)
    ) AS wsy ON kt.insure_company_channel = wsy.insure_company_channel AND kt.data_dt = wsy.data_dt
    -- 关联claim_ods.dim_insure_company_channel表获取渠道名称
    LEFT JOIN claim_ods.dim_insure_company_channel cic ON kt.insure_company_channel = cic.channel_key
ORDER BY
    日期 DESC;

"""


def truncate_table(table_name='CLAIM_DWD.DWD_YSJ_LR_COST_STATISTICS'):
    with DatabaseConnection() as conn:
        truncate_sql = f"TRUNCATE TABLE {table_name}"
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            conn.commit()


def insert_data(sql_query):
    with DatabaseConnection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()


if __name__ == "__main__":
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序开始时间：", start_time)
    truncate_table()
    insert_data(sql_query)
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("程序结束时间：", end_time)
