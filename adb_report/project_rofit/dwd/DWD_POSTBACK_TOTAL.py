# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta


today = date.today()

# 计算昨天的日期
yesterday = today - timedelta(days=1)


sql_query = f"""
  -- @description: 回传汇总数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-06-20 15:01:06
  -- @author: 01
  -- @version: 1.0.0
INSERT INTO claim_dwd.DWD_POSTBACK_TOTAL (
    insure_company_channel,
    department_code,
    product_type,
    back_time,
    claim_source,
    ACCEPT_NUM,
    app_no,
    postback_way,
    bill_no,
    treatment_date,
    input_company_code,
    is_back,
    data_dt
)
-- 首先定义WITH子查询
WITH t1 AS (
#      select a.insure_company_channel,
#              a.department_code,
#              a.product_type ,
#              pr.back_time ,
#              a.claim_source,
#              a.ACCEPT_NUM,
#              c.claim_no app_no  ,
#              pr.postback_way,
#              b.bill_no,
#              b.treatment_date,
#              d.input_company_code,
#              '结案' is_back
#         from claim_ods.accept_list_record a
#         left join claim_ods.claim c
#           on a.accept_num = c.acceptance_no  and c.insure_company_channel = 'PA02'
#          and c.delete_flag = '0'
#        inner join claim_ods.postback_record pr
#           on pr.app_no = c.claim_no
#          and pr.insure_company_channel = 'PA02'
#          and pr.back_status in ('2', '21')
#          and pr.is_deleted = 'N'
#         LEFT JOIN claim_ods.bill b ON b.claim_id = c.id  and b.insure_company_channel = 'PA02'
#         left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
#        where a.insure_company_channel = 'PA02'  and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY) and a.DEL_FLAG = '0'
# 
# 
  select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
             pr.back_time    ,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no   ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' as is_back
        from claim_ods.accept_list_record a
       left join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'TK11'
         and c.delete_flag = '0'
       left join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'TK11'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
         LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'TK11'
       where a.insure_company_channel ='TK11'  and a.DEL_FLAG = '0'
  and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY)


union  all
# 泰康电力

  select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no   ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'TK02'
         and c.delete_flag = '0'
       inner join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.receiver = 'I' and pr.insure_company_channel = 'TK02' and pr.back_status in ('2', '21')
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'TK02'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='TK02'  and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY) and a.DEL_FLAG = '0'

# 泰康北分

union  all
  select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
            case when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then c.cancle_time
                   when pr.back_status in ('2','21') then pr.back_time  else '1900-01-01' end  back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no   ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
                         case
             when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then
              '撤案'
             when pr.back_status in ('2','21') then
              '结案'
             else
              ''
           end as is_back
        from claim_ods.accept_list_record a
       left join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'TK04'
         and c.delete_flag = '0'
       left join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'TK04'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'TK04'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='TK04'  and a.DEL_FLAG = '0'

union all
## 泰康江苏
  select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
            case when (c.clm_process_status = '11'  or a.ACCEPT_STATUS='5')  and  c.cancle_time is not null then c.cancle_time
                when (c.clm_process_status = '11'  or a.ACCEPT_STATUS='5')  and  c.cancle_time is  null then a.T_UPD_TIME
                   when pr.back_status in ('2','21') then pr.back_time  else '1900-01-01' end  back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no   ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
                         case
             when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then
              '撤案'
             when pr.back_status in ('2','21') then
              '结案'
             else
              ''
           end as is_back
        from claim_ods.accept_list_record a
       left join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'TK09'
         and c.delete_flag = '0'
       left join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'TK09'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'TK09'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='TK09'  and a.DEL_FLAG = '0'
UNION ALL
## 泰康辽宁
  select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
            case when (c.clm_process_status = '11'  or a.ACCEPT_STATUS='5')  and  c.cancle_time is not null then c.cancle_time
                when (c.clm_process_status = '11'  or a.ACCEPT_STATUS='5')  and  c.cancle_time is  null then a.T_UPD_TIME
                   when pr.back_status in ('2','21') then pr.back_time  else '1900-01-01' end  back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no   ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
                         case
             when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then
              '撤案'
             when pr.back_status in ('2','21') then
              '结案'
             else
              ''
           end as is_back
        from claim_ods.accept_list_record a
       left join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'TK10'
         and c.delete_flag = '0'
       left join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'TK10'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'TK10'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='TK10'  and a.DEL_FLAG = '0'
UNION ALL

  select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
            case when (c.clm_process_status = '11'  or a.ACCEPT_STATUS='5')  and  c.cancle_time is not null then c.cancle_time
                when (c.clm_process_status = '11'  or a.ACCEPT_STATUS='5')  and  c.cancle_time is  null then a.T_UPD_TIME
                   when pr.back_status in ('2','21') then pr.back_time  else '1900-01-01' end  back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no   ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
                         case
             when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then
              '撤案'
             when pr.back_status in ('2','21') then
              '结案'
             else
              ''
           end as is_back
        from claim_ods.accept_list_record a
       left join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'TK12'
         and c.delete_flag = '0'
       left join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'TK12'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'TK12'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='TK12'  and a.DEL_FLAG = '0'
  and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY)


union  all
  # 泰康上海

  select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
             case when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then c.cancle_time
                   when pr.back_status in ('2','21') then pr.back_time  else '1900-01-01' end  back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no   ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
                         case
             when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then
              '撤案'
             when pr.back_status in ('2','21')  then
              '结案'
             else
              ''
           end as is_back
        from claim_ods.accept_list_record a
       left join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'TK01'
         and c.delete_flag = '0'
       left join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'TK01'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'TK01'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='TK01' and a.DEL_FLAG = '0'


union  all


# 泰康河南

  select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
             case when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then c.cancle_time
                   when pr.back_status in ('2','21') then pr.back_time  else '1900-01-01' end  back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no   ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
                         case
             when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then
              '撤案'
             when pr.back_status in ('2','21')  then
              '结案'
             else
              ''
           end as is_back
        from claim_ods.accept_list_record a
       left join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'TK06'
         and c.delete_flag = '0'
       left join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'TK06'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'TK06'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='TK06' and a.DEL_FLAG = '0'

union  all
# 泰康广东
   select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              case when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then c.cancle_time
                   when pr.back_status in ('2','21') then pr.back_time  else '1900-01-01' end  back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
                         case
             when c.clm_process_status = '11'  or a.ACCEPT_STATUS='5' then
              '撤案'
             when pr.back_status in ('2','21')  then
              '结案'
             else
              ''
           end as is_back
        from claim_ods.accept_list_record a
       left join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'TK07'
         and c.delete_flag = '0'
       left join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'TK07'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'TK07'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='TK07' and a.DEL_FLAG = '0'


union  all
# 中智
         select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              fr.CREATE_TIME,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              '' postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'CP01'
         and c.delete_flag = '0'
         LEFT JOIN claim_ods.front_seq_record fr
          on fr.app_no = c.claim_no
         and fr.is_deleted = 'N'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'CP01'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='CP01'   and substr(fr.CREATE_TIME,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY) and a.DEL_FLAG = '0'

union  all
# 渤海

         select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              '' postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'BH01'
         and c.delete_flag = '0'
                inner join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'BH01'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'BH01'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='BH01'   and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY) and a.DEL_FLAG = '0'

union  all

# 太保健康
         select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              '' postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'CP08'
         and c.delete_flag = '0'
                inner join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'CP08'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'CP08'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='CP08'  and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY) and a.DEL_FLAG = '0'
union  all

# 大家养老

         select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              '' postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'DJ01'
         and c.delete_flag = '0'
                inner join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'DJ01'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'DJ01'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='DJ01'  and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY) and a.DEL_FLAG = '0'
union  all

# 太保宁波
         select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'CP02'
         and c.delete_flag = '0'
                inner join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'CP02'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'CP02'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='CP02'   and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY) and a.DEL_FLAG = '0'
union  all

# 太保财产 苏州分公司

                  select
             a.department_code insure_company_channel,
             '' department_code,
             a.product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'CP10'
         and c.delete_flag = '0'
       inner join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'CP10'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'CP10'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='CP10'   and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY)
        and pr.back_status in ('2', '21') and a.DEL_FLAG = '0'
# union  all
# # 太保财产 上海分公司
#           select
#               a.insure_company_channel,
#              '' department_code,
#              a.product_type ,
#               pr.back_time,
#               a.claim_source,
#               a.ACCEPT_NUM,
#               c.claim_no app_no  ,
#               pr.postback_way,
#               b.bill_no,
#               b.treatment_date,
#               d.input_company_code,
#               '结案' is_back
#         from claim_ods.accept_list_record a
#        inner join claim_ods.claim c
#           on a.accept_num = c.acceptance_no and c.insure_company_channel = 'CP07'
#          and c.delete_flag = '0'
#        inner join claim_ods.postback_record pr
#           on pr.app_no = c.claim_no
#          and pr.is_deleted = 'N'
#          and pr.insure_company_channel = 'CP07'
#         LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'CP07'
#         left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
#        where a.insure_company_channel ='CP07'   and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY)
#         and pr.back_status in ('2', '21')   and a.DEL_FLAG = '0'


       union  all

# 众安暖哇
             select
            a.insure_company_channel,
             a.department_code,
             a.product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              pr.postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'ZA01'
         and c.delete_flag = '0'
       left join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'ZA01'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'ZA01'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='ZA01'   and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY)  and pr.back_status in ('2', '21')   and  a.DEL_FLAG = '0'

         union  all

# 中国人寿财产保险

                select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              '' postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'GS01'
         and c.delete_flag = '0'
                inner join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'GS01'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'GS01'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='GS01'  and substr(pr.back_time,1,10) >= '2024-10-01'
          and a.DEL_FLAG = '0' and pr.back_status in ('2', '21')

    union  all
    # 中国人民财产保险
         select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              a.T_CRT_TIME,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              '' postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'RB01'
         and c.delete_flag = '0'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'RB01'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='RB01'  and substr(a.T_CRT_TIME,1,10) >= '2024-09-01' and a.DEL_FLAG = '0'
       union  all
             # 长生人寿

         select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              '' postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'CS01'
         and c.delete_flag = '0'
                inner join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'CS01'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'CS01'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='CS01'  and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY) and a.DEL_FLAG = '0'
         AND  PR.task_type=0
         UNION ALL  
         
         # 太保高端
                  select
            a.insure_company_channel,
             '' department_code,
             '' product_type ,
              pr.back_time,
              a.claim_source,
              a.ACCEPT_NUM,
              c.claim_no app_no  ,
              '' postback_way,
              b.bill_no,
              b.treatment_date,
              d.input_company_code,
              '结案' is_back
        from claim_ods.accept_list_record a
       inner join claim_ods.claim c
          on a.accept_num = c.acceptance_no and c.insure_company_channel = 'CP11'
         and c.delete_flag = '0'
                inner join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.insure_company_channel = 'CP11'
        LEFT JOIN claim_ods.bill b ON b.claim_id = c.id and b.insure_company_channel = 'CP11'
        left join claim_ods.image_assign_task d on a.accept_batch_no = d.accept_batch_no
       where a.insure_company_channel ='CP11'  and substr(pr.back_time,1,10) >= (CURRENT_DATE - INTERVAL '100' DAY) and a.DEL_FLAG = '0'

)
SELECT
    insure_company_channel,
    department_code,
    product_type,
    back_time ,
    claim_source,
    ACCEPT_NUM,
    app_no,
    postback_way,
    bill_no,
    treatment_date ,
    input_company_code,
    is_back,
    REPLACE(CURDATE(), "-", "") AS data_dt
FROM t1
WHERE DATE_FORMAT(back_time, '%Y-%m-%d') >= (CURRENT_DATE - INTERVAL '100' DAY)
and SUBSTR(back_time, 1, 10) <= '{yesterday}' ;
"""
def truncate_table(table_name='CLAIM_DWD.DWD_POSTBACK_TOTAL'):
    with DatabaseConnection() as conn:
        truncate_sql = f"DELETE FROM {table_name}  WHERE DATE_FORMAT(back_time, '%Y-%m-%d') >= (CURRENT_DATE - INTERVAL '100' DAY);"
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
