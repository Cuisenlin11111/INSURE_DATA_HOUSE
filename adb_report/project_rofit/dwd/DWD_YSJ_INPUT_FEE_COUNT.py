# import sys
#
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, timedelta

# 获取当前日期时间
current_date = datetime.now()
# 计算6个月前的日期
six_months_ago_date = current_date - timedelta(days=20)
six_months = six_months_ago_date.strftime('%Y-%m')

sql_query = f"""
  -- @description: 因朔桔成本报表(回传维度)
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2025-01-06 15:01:06
  -- @author: 01
  -- @version: 1.0.0
insert into CLAIM_DWD.DWD_YSJ_INPUT_FEE_COUNT
SELECT
    -- 通过关联获取渠道名称
    cic.channel_value AS 渠道,
    -- 日期字段
    COALESCE(kt.data_dt, ocr.data_dt, hsbx_y.data_dt, hsbx_n.data_dt, inp.data_dt, rev.data_dt, wsy.data_dt) AS 日期,

    cast(COALESCE(kt.kt_num, 0) * 0.07 as DECIMAL(10, 2)) AS 快瞳电子票调用费用,

    cast(COALESCE(ocr.ocr_num, 0) * 0.15 as DECIMAL(10, 2)) AS ocr调用费用,

    cast(COALESCE(hsbx_y.hsbx_y_num, 0) * 0.12 as DECIMAL(10, 2)) AS 海盛宝信电子票调无明细费用,

    cast(COALESCE(hsbx_n.hsbx_n_num, 0) * 0.20 as DECIMAL(10, 2)) AS 海盛宝信电子票有明细费用,

    cast(COALESCE(inp.inp_count, 0) * 0.45 as DECIMAL(10, 2)) AS 录入费用,

    cast(COALESCE(rev.rev_count, 0) * 0.21 as DECIMAL(10, 2)) AS 复核费用,

    cast(COALESCE(wsy.wsy_num, 0) * 0.05 as DECIMAL(10, 2)) AS 五要素费用
FROM
    -- 快瞳电子票调用次数的子查询，作为主表进行左连接
    (
        SELECT
            substr(at.case_id, 1, 4) insure_company_channel,
            substr(pr.back_time, 1, 10) data_dt,
            count(1) kt_num
        FROM
            inphile_ods.algorithm_task  at
        lEFT JOIN claim_ods.postback_record pr
        on at.case_id=pr.app_no and pr.is_deleted='N' AND PR.receiver='I'   and  pr.task_type=0
        WHERE
            at.channel_type IN ('2', '3', '4', '7', '9', '12')
        AND substr(at.case_id, 1, 4)<>'CP01'
        AND PR.back_status='2'
        and substr(pr.back_time, 1, 7)>='{six_months}'
        GROUP BY
            substr(at.case_id, 1, 4), substr(pr.back_time, 1, 10)
        union all
        SELECT
            substr(at.case_id, 1, 4) insure_company_channel,
            substr(fr.CREATE_TIME, 1, 10) data_dt,
            count(1) kt_num
        FROM
            inphile_ods.algorithm_task  at
        LEFT JOIN claim_ods.front_seq_record fr
          on fr.app_no = at.case_id
         and fr.is_deleted = 'N'
        WHERE
            at.channel_type IN ('2', '3', '4', '7', '9', '12')
        AND substr(at.case_id, 1, 4)='CP01'
         and substr(fr.CREATE_TIME, 1, 7)>='{six_months}'
        GROUP BY
            substr(at.case_id, 1, 4), substr(fr.CREATE_TIME, 1, 10)
    ) AS kt
    -- 左连接ocr调用次数的子查询
    LEFT JOIN
    (
        SELECT
            cin.insure_company_channel,
            substr(pr.back_time, 1, 10) data_dt,
            count(1) ocr_num
        FROM
            inphile_ods.white_board_result wbr
        LEFT JOIN
            inphile_ods.case_image   cim  ON wbr.image_id = cim.id
        left join
            claim_ods.postback_record pr  on cim.case_id=pr.app_no and pr.is_deleted='N'   and  pr.task_type=0
        LEFT JOIN
            inphile_ods.case_info cin ON cim.case_id = cin.id
        WHERE
            wbr.deleted = 0  and   substr(cim.case_id, 1, 4)<>'CP01'  AND PR.back_status='2'  and substr(pr.back_time, 1, 7)>='{six_months}'
        GROUP BY
            cin.insure_company_channel, substr(pr.back_time, 1, 10)
        union all
                SELECT
            cin.insure_company_channel,
            substr(fr.CREATE_TIME, 1, 10) data_dt,
            count(1) ocr_num
        FROM
            inphile_ods.white_board_result wbr
        LEFT JOIN
            inphile_ods.case_image   cim  ON wbr.image_id = cim.id
        left join
            claim_ods.front_seq_record fr  on fr.app_no = cim.case_id  and fr.is_deleted = 'N'
        LEFT JOIN
            inphile_ods.case_info cin ON cim.case_id = cin.id
        WHERE
            wbr.deleted = 0  and   substr(cim.case_id, 1, 4)='CP01'   and substr(fr.CREATE_TIME, 1, 7)>='{six_months}'
        GROUP BY
            cin.insure_company_channel, substr(fr.CREATE_TIME, 1, 10)
    ) AS ocr ON kt.insure_company_channel = ocr.insure_company_channel AND kt.data_dt = ocr.data_dt
    -- 左连接海盛宝信电子票（无明细）的子查询
    LEFT JOIN
    (
       SELECT
            substr(at.case_id, 1, 4) insure_company_channel,
            substr(pr.back_time, 1, 10) data_dt,
            count(1) hsbx_y_num
        FROM
            inphile_ods.algorithm_task  at
        lEFT JOIN claim_ods.postback_record pr
        on at.case_id=pr.app_no and pr.is_deleted='N'   AND PR.receiver='I'   and  pr.task_type=0
        WHERE
            at.channel_type IN ('8', '13')
        AND substr(at.case_id, 1, 4)<>'CP01' and substr(pr.back_time, 1, 7)>='{six_months}'
        AND PR.back_status='2' and at.has_detail = 2
        GROUP BY
            substr(at.case_id, 1, 4), substr(pr.back_time, 1, 10)
        union all
        SELECT
            substr(at.case_id, 1, 4) insure_company_channel,
            substr(fr.CREATE_TIME, 1, 10) data_dt,
            count(1) hsbx_y_num
        FROM
            inphile_ods.algorithm_task  at
        LEFT JOIN claim_ods.front_seq_record fr
          on fr.app_no = at.case_id
         and fr.is_deleted = 'N'
        WHERE
            at.channel_type IN ('8', '13')
        AND substr(at.case_id, 1, 4)='CP01'   and at.has_detail = 2  and substr(fr.CREATE_TIME, 1, 7)>='{six_months}'
        GROUP BY
            substr(at.case_id, 1, 4), substr(fr.CREATE_TIME, 1, 10)


    ) AS hsbx_y ON kt.insure_company_channel = hsbx_y.insure_company_channel AND kt.data_dt = hsbx_y.data_dt
    -- 左连接海盛宝信电子票（有明细）的子查询
    LEFT JOIN
    (
         SELECT
            substr(at.case_id, 1, 4) insure_company_channel,
            substr(pr.back_time, 1, 10) data_dt,
            count(1) hsbx_n_num
        FROM
            inphile_ods.algorithm_task  at
        lEFT JOIN claim_ods.postback_record pr
        on at.case_id=pr.app_no and pr.is_deleted='N'   AND PR.receiver='I'   and  pr.task_type=0
        WHERE
            at.channel_type IN ('8', '13')
        AND substr(at.case_id, 1, 4)<>'CP01'  and substr(pr.back_time, 1, 7)>='{six_months}'
        AND PR.back_status='2' and at.has_detail = 1
        GROUP BY
            substr(at.case_id, 1, 4), substr(pr.back_time, 1, 10)
        union all
        SELECT
            substr(at.case_id, 1, 4) insure_company_channel,
            substr(fr.CREATE_TIME, 1, 10) data_dt,
            count(1) hsbx_n_num
        FROM
            inphile_ods.algorithm_task  at
        LEFT JOIN claim_ods.front_seq_record fr
          on fr.app_no = at.case_id
         and fr.is_deleted = 'N'
        WHERE
            at.channel_type IN ('8', '13')
        AND substr(at.case_id, 1, 4)='CP01'   and at.has_detail = 1   and substr(fr.CREATE_TIME, 1, 7)>='{six_months}'
        GROUP BY
            substr(at.case_id, 1, 4), substr(fr.CREATE_TIME, 1, 10)

    ) AS hsbx_n ON kt.insure_company_channel = hsbx_n.insure_company_channel AND kt.data_dt = hsbx_n.data_dt
    -- 左连接录入次数的子查询
    LEFT JOIN
    (
        SELECT
            substr(ci.id, 1, 4) as insure_company_channel,
            substr(pr.back_time, 1, 10) data_dt,
            COUNT(DISTINCT cb.id) AS inp_count
        FROM
            inphile_ods.case_info  ci
        LEFT JOIN
            inphile_ods.case_log cl ON ci.id = cl.case_id
        left join
                claim_ods.postback_record pr on ci.id=pr.app_no and pr.is_deleted='N' and pr.receiver='I'   and  pr.task_type=0
        LEFT JOIN
            inphile_ods.account acc ON cl.user_id = acc.account
        LEFT JOIN
            inphile_ods.case_apply ca  ON ca.case_id = ci.id
        LEFT JOIN
            inphile_ods.case_bill  cb ON cb.apply_id = ca.id
        WHERE
            cl.node = '录入' AND cl.handle = '账核诊录入'
            AND ci.deleted = 0 and substr(ci.id, 1, 4)<>'CP01' and pr.back_status='2' and substr(pr.back_time, 1, 7)>='{six_months}'
        GROUP BY
            substr(ci.id, 1, 4), substr(pr.back_time, 1, 10)
        union all
                SELECT
            substr(ci.id, 1, 4) as insure_company_channel,
            substr(fr.create_time, 1, 10) data_dt,
            COUNT(DISTINCT cb.id) AS inp_count
        FROM
            inphile_ods.case_info  ci
        LEFT JOIN
            inphile_ods.case_log cl ON ci.id = cl.case_id
        left join
                claim_ods.front_seq_record fr on fr.app_no=ci.id  and fr.is_deleted = 'N'
        LEFT JOIN
            inphile_ods.account acc ON cl.user_id = acc.account
        LEFT JOIN
            inphile_ods.case_apply ca  ON ca.case_id = ci.id
        LEFT JOIN
            inphile_ods.case_bill  cb ON cb.apply_id = ca.id
        WHERE
            cl.node = '录入' AND cl.handle = '账核诊录入'
            AND ci.deleted = 0 and substr(ci.id, 1, 4)='CP01'   and substr(fr.CREATE_TIME, 1, 7)>='{six_months}'
        GROUP BY
            substr(ci.id, 1, 4), substr(fr.create_time, 1, 10)
    ) AS inp ON kt.insure_company_channel = inp.insure_company_channel AND kt.data_dt = inp.data_dt
    -- 左连接复核次数的子查询
    LEFT JOIN
    (
       SELECT
            substr(ci.id, 1, 4) as insure_company_channel,
            substr(pr.back_time, 1, 10) data_dt,
            COUNT(DISTINCT cb.id) AS rev_count
        FROM
            inphile_ods.case_info  ci
        LEFT JOIN
            inphile_ods.case_log cl ON ci.id = cl.case_id
        left join
                claim_ods.postback_record pr on ci.id=pr.app_no and pr.is_deleted='N'   AND PR.receiver='I'   and  pr.task_type=0
        LEFT JOIN
            inphile_ods.account acc ON cl.user_id = acc.account
        LEFT JOIN
            inphile_ods.case_apply ca  ON ca.case_id = ci.id
        LEFT JOIN
            inphile_ods.case_bill  cb ON cb.apply_id = ca.id
        WHERE
            cl.node IN ('复核', '质检') AND cl.handle IN ('复核录入', '质检录入')
            AND ci.deleted = 0 and substr(ci.id, 1, 4)<>'CP01' and pr.back_status='2'  and substr(pr.back_time, 1, 7)>='{six_months}'
        GROUP BY
            substr(ci.id, 1, 4), substr(pr.back_time, 1, 10)
        union all
                SELECT
            substr(ci.id, 1, 4) as insure_company_channel,
            substr(fr.create_time, 1, 10) data_dt,
            COUNT(DISTINCT cb.id) AS rev_count
        FROM
            inphile_ods.case_info  ci
        LEFT JOIN
            inphile_ods.case_log cl ON ci.id = cl.case_id
        left join
                claim_ods.front_seq_record fr on fr.app_no=ci.id  and fr.is_deleted = 'N'
        LEFT JOIN
            inphile_ods.account acc ON cl.user_id = acc.account
        LEFT JOIN
            inphile_ods.case_apply ca  ON ca.case_id = ci.id
        LEFT JOIN
            inphile_ods.case_bill  cb ON cb.apply_id = ca.id
        WHERE
            cl.node IN ('复核', '质检') AND cl.handle IN ('复核录入', '质检录入')
            AND ci.deleted = 0 and substr(ci.id, 1, 4)='CP01'   and substr(fr.CREATE_TIME, 1, 7)>='{six_months}'
        GROUP BY
            substr(ci.id, 1, 4), substr(fr.create_time, 1, 10)
    ) AS rev ON kt.insure_company_channel = rev.insure_company_channel AND kt.data_dt = rev.data_dt
    -- 左连接五要素次数的子查询
    LEFT JOIN
    (
        SELECT
            substr(at.case_id, 1, 4) insure_company_channel,
            substr(pr.back_time, 1, 10) data_dt,
            count(1) wsy_num
        FROM
            inphile_ods.algorithm_task  at
        lEFT JOIN claim_ods.postback_record pr
                on at.case_id=pr.app_no and pr.is_deleted='N'   AND PR.receiver='I'   and  pr.task_type=0
        WHERE
            at.channel_type IN ('12', '13')
            AND at.task_type = 10
            AND substr(at.case_id, 1, 4)<>'CP01' and substr(pr.back_time, 1, 7)>='{six_months}'
            AND PR.back_status='2'
        GROUP BY
            substr(at.case_id, 1, 4), substr(pr.back_time, 1, 10)
        union all
        SELECT
            substr(at.case_id, 1, 4) insure_company_channel,
            substr(fr.create_time, 1, 10) data_dt,
            count(1) wsy_num
        FROM
            inphile_ods.algorithm_task  at
        LEFT JOIN claim_ods.front_seq_record fr
          on fr.app_no = at.case_id
         and fr.is_deleted = 'N'
        WHERE
            at.channel_type IN ('12', '13')
            AND at.task_type = 10
            AND substr(at.case_id, 1, 4)='CP01'   and substr(fr.CREATE_TIME, 1, 7)>='{six_months}'
        GROUP BY
            substr(at.case_id, 1, 4), substr(fr.create_time, 1, 10)
    ) AS wsy ON kt.insure_company_channel = wsy.insure_company_channel AND kt.data_dt = wsy.data_dt
    -- 关联claim_ods.dim_insure_company_channel表获取渠道名称
    LEFT JOIN claim_ods.dim_insure_company_channel cic ON kt.insure_company_channel = cic.channel_key
where  COALESCE(kt.data_dt, ocr.data_dt, hsbx_y.data_dt, hsbx_n.data_dt, inp.data_dt, rev.data_dt, wsy.data_dt)  is not null
ORDER BY
    日期 DESC;
"""


def truncate_table(table_name='CLAIM_DWD.DWD_YSJ_INPUT_FEE_COUNT'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete from  {table_name}  where  substring(data_dt,1,7)>='{six_months}' "
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
