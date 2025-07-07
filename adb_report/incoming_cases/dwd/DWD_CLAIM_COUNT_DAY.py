# import sys
# sys.path.append(r"E:\pycharm\database")
from database import DatabaseConnection
from datetime import datetime, date, timedelta

today = date.today()

sql_query = f"""
  -- @description: 案件量统计报表基础数据
  -- @param:
  -- @return:
  -- @alter:
  -- @time: 2024-08-29 15:01:06
  -- @author: 01
  -- @version: 1.0.0



 insert into CLAIM_DWD.DWD_CLAIM_COUNT_DAY
    (insure_company_channel,
     gmt_created,
     jjl,
     cancel_vol,
     xxqd,
     xsqd,
     lrl,
     drhcal,
     blchcal,
     qlchcal,
     perc_all_flow,
     hcsbal,
     zdshal,
	 ZDSHAL_RW,
     shzdhl,
     shzdhl_rw,
     fh_auto_claim_count,
     fhzdhl,
     question_claim_vol,
     hospatal_claim_vol,
     data_dt,
     sh_claim_count)

  with tmp_11 as (
    select CASE  WHEN insure_company_channel = 'CP10' THEN DEPARTMENT_CODE
          ELSE insure_company_channel
         END insure_company_channel,
               substr(T_CRT_TIME,1,10)   T_CRT_TIME,
               substr(ACCEPT_DATE,1,10)  ACCEPT_DATE,
             accept_num,
               ACCEPT_STATUS,
               claim_source
  from claim_ods.accept_list_record
      where DEL_FLAG = '0'
    and insure_company_channel != 'common'
      # and insure_company_channel != 'PA02'
      and insure_company_channel != 'CP01'
      ),
        a1 as (select
  insure_company_channel,
  count(*) jjl,
  T_CRT_TIME comm_date
  from tmp_11
 where  insure_company_channel in ('TK01'  , 'TK02', 'TK04' , 'TK06' , 'TK07', 'TK09', 'TK10', 'TK12','TK11','PA02','CS01','大连分公司','苏州分公司','上海分公司','YH01')   and ACCEPT_STATUS<>'1'
  and   length(ACCEPT_NUM) <> 24
  group by insure_company_channel,T_CRT_TIME
        union all
    select
         insure_company_channel,
         COUNT(*) jjl,
         ACCEPT_DATE comm_date
    from tmp_11
    where  insure_company_channel in ('DJ01','ZA01','CP08','CP11','GS01','CP11')
    group by insure_company_channel,ACCEPT_DATE
    union all
     select
         insure_company_channel,
         COUNT(*) jjl,
         ACCEPT_DATE comm_date
    from tmp_11
    where  insure_company_channel not in ('DJ01','ZA01','CP08','CP11','PA02','CS01','TK01'  , 'TK09', 'TK10', 'TK12','TK11','TK02', 'TK04' , 'TK06' , 'TK07','大连分公司','苏州分公司','上海分公司','GS01','YH01','CP11')
    and ACCEPT_STATUS<>'1'
    group by insure_company_channel,ACCEPT_DATE),
        ca as ( SELECT
alr.INSURE_COMPANY_CHANNEL ,
 case when c.cancle_time  is null  then substring(alr.T_UPD_TIME,1,10)  else  substring(c.cancle_time,1,10)  end  comm_date,
  count(distinct alr.ACCEPT_NUM ) cancel_vol
 FROM claim_ods.accept_list_record alr
LEFT JOIN claim_ods.claim c on c.acceptance_no = alr.ACCEPT_NUM
WHERE
 alr.DEL_FLAG = '0'
 and (alr.ACCEPT_STATUS = '5' or c.clm_process_status = '11'
      # and alr.insure_company_channel != 'PA02'
      and alr.insure_company_channel != 'CP01'
     )
GROUP BY comm_date,alr.INSURE_COMPANY_CHANNEL),
            a2 as (select
  insure_company_channel,
  count(*) xxqd,
  T_CRT_TIME comm_date
  from tmp_11
 where  insure_company_channel in ('TK01'  , 'TK02', 'TK04' , 'TK06' , 'TK07', 'TK09', 'TK10', 'TK12','TK11','PA02','CS01','大连分公司','苏州分公司','上海分公司','YH01')  and ACCEPT_STATUS<>'1'  and claim_source  in ( '1','3')
  group by insure_company_channel,T_CRT_TIME
        union all
    select
         insure_company_channel,
         COUNT(*) xxqd,
         ACCEPT_DATE comm_date
    from tmp_11
    where  insure_company_channel in ('DJ01','ZA01','CP08','CP11','GS01','CP11') and claim_source  in ( '1','3')
    group by insure_company_channel,ACCEPT_DATE
    union all
     select
         insure_company_channel,
         COUNT(*) xxqd,
         ACCEPT_DATE comm_date
    from tmp_11
    where  insure_company_channel not in ('DJ01','ZA01','CP08','CP11','GS01','CP11','TK01'  , 'TK02', 'TK04' , 'TK06' , 'TK07', 'TK09', 'TK10', 'TK12','TK11','PA02','CS01','大连分公司','上海分公司','苏州分公司','YH01')
    and ACCEPT_STATUS<>'1' and claim_source in ( '1','3')
    group by insure_company_channel,ACCEPT_DATE),
        a3 as (select
  insure_company_channel,
  count(*) xsqd,
  T_CRT_TIME comm_date
  from tmp_11
 where  insure_company_channel in ('TK01'  , 'TK02', 'TK04' , 'TK06' , 'TK07', 'TK09', 'TK10', 'TK12','TK11','PA02','CS01','大连分公司','上海分公司','苏州分公司','YH01')  and ACCEPT_STATUS<>'1'  and claim_source in ( '2','4')
  group by insure_company_channel,T_CRT_TIME
        union all
    select
         insure_company_channel,
         COUNT(*) xsqd,
         ACCEPT_DATE comm_date
    from tmp_11
    where  insure_company_channel in ('DJ01','ZA01','CP08','CP11','GS01','CP11') and claim_source in ( '2','4')
    group by insure_company_channel,ACCEPT_DATE
    union all
     select
         insure_company_channel,
         COUNT(*) xsqd,
         ACCEPT_DATE comm_date
    from tmp_11
    where  insure_company_channel not in ('DJ01','ZA01','CP08','CP11','GS01','CP11','TK01'  , 'TK02', 'TK04' , 'TK06' , 'TK07', 'TK09', 'TK10', 'TK12','TK11','PA02','CS01','大连分公司','上海分公司','苏州分公司','YH01')
    and ACCEPT_STATUS<>'1' and claim_source in ( '2','4')
    group by insure_company_channel,ACCEPT_DATE),
        a4 as  (        select
               substr(c.create_time,1,10) comm_date,
               a.insure_company_channel,
             count(*) as lrl
          FROM tmp_11 a
          join claim_ods.claim c
            on a.accept_num = c.acceptance_no
         where c.delete_flag = '0'
         group by substr(c.create_time,1,10),
                  a.insure_company_channel),
  tmp_22 as (
    select   substr(pr.back_time,1,10)  back_time,
             CASE
          WHEN pr.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
          ELSE pr.insure_company_channel
         END insure_company_channel,
             pr.app_no,
             pr.postback_way,
             pr.accept_num,
             pr.receiver,
             pr.back_status,
             substr(gmt_created,1,10)  gmt_created
    from claim_ods.postback_record pr
          LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
        ON pr.accept_num = alro.accept_num AND alro.DEL_FLAG = '0'
         where pr.back_status in ('2', '21','3')
           and pr.is_deleted = 'N'
                       and pr.task_type=0
               # and pr.insure_company_channel != 'PA02'
                 and pr.insure_company_channel != 'CP01'
         ),
        a5 as (       select
               substr(back_time,1,10) comm_date,
               pr.insure_company_channel,
             count(distinct pr.app_no) as drhcal
          from tmp_22 pr
         where  pr.insure_company_channel not in
               ('TK02', 'CP01', 'YX01', 'common') and  pr.back_status in ('2', '21')
         group by substr(back_time,1,10),
                  pr.insure_company_channel
      union all
      ##泰康山东的案件比较特殊，回传了两次，一次回传电力，一次回传泰康  注意：  泰康山东的回传案件 加receiver ='I' 只保留回传泰康的量
      select
             substr(back_time,1,10) comm_date,
             insure_company_channel,
             count(distinct pr.app_no) as drhcal
        from claim_ods.postback_record pr
       where pr.back_status in ('2', '21')
         and pr.is_deleted = 'N'
          and receiver ='I'
         and insure_company_channel = 'TK02'
       group by substr(back_time,1,10),
                insure_company_channel
      union all
      ##宁波普惠
      select
             substr(cat.T_UPD_TM,1,10) comm_date,
             c.insure_company_channel,
             count(DISTINCT c.id) drhcal
        FROM claim_ods.accept_list_record a
        join claim_ods.claim c
          on a.accept_num = c.acceptance_no
         and c.delete_flag = '0'
        left join claim_ods.case_audit_task cat
          on cat.C_CLAIM_CASE_NO = c.claim_no
         and cat.C_DEL_FLAG = '0'
         and cat.C_SUB_STATUS = '79'
       where c.clm_process_status = '9'
         and c.insure_company_channel = 'YX01'
       group by substr(cat.T_UPD_TM,1,10),
                c.insure_company_channel
),
        a6 as (       select
                 pr.insure_company_channel,
               back_time  comm_date,
             count(DISTINCT pr.app_no) as blchcal
          from tmp_22 pr
         WHERE  pr.postback_way = 'H' and  pr.back_status in ('2', '21')
         group by back_time,
          pr.insure_company_channel),
          gg as (
SELECT DISTINCT
    pr.app_no
FROM
    claim_ods.postback_record pr
LEFT JOIN claim_ods.clm_app_info cai
    ON cai.C_CUSTOM_APP_NO = pr.app_no
    AND cai.C_DEL_FLAG = '0'
    AND cai.INSURANCE_COMPANY = 'CP08'
LEFT JOIN claim_ods.claim_policy cp
    ON cp.policy_no = cai.C_PLY_NO
LEFT JOIN claim_ods.ply p
    ON cp.group_policy_no = p.C_PLY_NO
    AND p.C_DEL_FLAG = '0'
WHERE
    p.product_type IN ('CID', 'CI', 'AI', 'OTHER', 'MI')
    AND pr.insure_company_channel = 'CP08'
    AND pr.back_status IN ('2', '21')
   and pr.is_deleted='N'
),
        a7 as ( ##全流程回传案件量，中智是单独的计算逻辑

                select back_time comm_date,
                    COUNT(DISTINCT  pr.app_no) as qlchcal,
                 pr.insure_company_channel
          from tmp_22 pr
    LEFT JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = pr.app_no AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY = 'CP08'
    LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = cai.C_PLY_NO
    left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO and p.C_DEL_FLAG = '0'
    left join gg ON gg.app_no = pr.app_no
         where  (pr.postback_way = 'W' or pr.postback_way is null)
           and pr.insure_company_channel NOT in ('CP01','YX01','TK02') and pr.back_status in ('2', '21')
           and  gg.app_no is null
         group by back_time,
                   pr.insure_company_channel
        UNION ALL
      ## 泰康电力
       select back_time comm_date,
                count(distinct pr.app_no) as qlchcal,
                 pr.insure_company_channel
          from tmp_22 pr
         where  (pr.postback_way = 'W' or pr.postback_way is null)
           and pr.insure_company_channel ='TK02' and pr.back_status in ('2', '21')
           and receiver='I'
         group by back_time,
                   pr.insure_company_channel
    ) ,    a8 as
     (
          select gmt_created comm_date,
               count(DISTINCT pr.app_no) as hcsbal,
               pr.insure_company_channel
          from tmp_22 pr
         WHERE pr.back_status in ('3')
           and receiver = 'I'
         group by gmt_created,
                  pr.insure_company_channel
      ),
    a9 as
     (
             SELECT
        SUM(zdshal) zdshal,
        comm_date,
        insure_company_channel
      FROM
      (
        select
              COUNT(DISTINCT  pr.app_no) zdshal,
               substr(pr.back_time,1,10) comm_date,
               CASE
          WHEN pr.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
          ELSE pr.insure_company_channel
         END insure_company_channel
          FROM claim_ods.postback_record pr
          LEFT JOIN claim_ods.case_audit_task cat
            on cat.c_claim_case_no = pr.app_no

          LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
      ON pr.accept_num = alro.accept_num AND alro.DEL_FLAG = '0'
        LEFT JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = pr.app_no AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY = 'CP08'
        LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = cai.C_PLY_NO
       left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO and p.C_DEL_FLAG = '0'
       left  join gg ON gg.app_no = pr.app_no
         WHERE pr.back_status in ('2', '21')
           and pr.is_deleted = 'N'
           and pr.insure_company_channel not in ('CP01', 'YX01')
           and pr.receiver = 'I'
           and cat.C_HANDLE_CDE = '1'
           and gg.app_no is null
             and pr.task_type=0
         group by substr(pr.back_time,1,10),
                  pr.insure_company_channel,
                  alro.DEPARTMENT_CODE
       )
       group by insure_company_channel,comm_date
    ),
    a10 as
     (
          SELECT
        insure_company_channel,
        comm_date,
        SUM(fh_auto_claim_count) as fh_auto_claim_count
      FROM
      (
        select
          CASE
          WHEN pr.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
          ELSE pr.insure_company_channel
         END insure_company_channel,
              substr(pr.back_time,1,10) comm_date,
              count(DISTINCT a.accept_num) fh_auto_claim_count
          from claim_ods.accept_list_record a
          join claim_ods.claim c
            on a.accept_num = c.acceptance_no
           and c.delete_flag = '0'
          join claim_ods.postback_record pr
            on pr.app_no = c.claim_no
           and pr.is_deleted = 'N'
           and pr.back_status in ('2', '21')
           and pr.receiver = 'I'
          join claim_ods.case_audit_task ca
            on ca.C_CLAIM_CASE_NO = c.claim_no
           and ca.C_DEL_FLAG = '0'
         where (pr.postback_way = 'W' or pr.postback_way is null)
           and ca.C_REVIEWER_STAFF = '系统自动'
           and pr.insure_company_channel not in ('CP01', 'YX01','TK07')
         and pr.task_type=0
         group by pr.insure_company_channel,
                  substr(pr.back_time,1,10),
                  a.DEPARTMENT_CODE
      )
        group by insure_company_channel,comm_date
        UNION ALL
        select pr.insure_company_channel,
             substr(pr.back_time,1,10) comm_date,
             count(DISTINCT a.accept_num) fh_auto_claim_count
        from claim_ods.accept_list_record a
        join claim_ods.claim c
          on a.accept_num = c.acceptance_no
         and c.delete_flag = '0'
        join claim_ods.postback_record pr
          on pr.app_no = c.claim_no
         and pr.is_deleted = 'N'
         and pr.back_status in ('2', '21')
         and pr.receiver = 'I'
        join claim_ods.case_audit_task ca
          on ca.C_CLAIM_CASE_NO = c.claim_no
         and ca.C_DEL_FLAG = '0'
       where ca.C_REVIEWER_STAFF = '系统自动'
         and pr.insure_company_channel  in ('TK07')
       group by pr.insure_company_channel,
                substr(pr.back_time,1,10)
    ),
    a11 as
     (
      SELECT
          insure_company_channel,
          comm_date,
          SUM(问题件案件数) 问题件案件数
        FROM
        (
        SELECT
           CASE
          WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
          ELSE c.insure_company_channel
         END insure_company_channel,
               substr(q.gmt_created,1,10) comm_date,
               count(DISTINCT c.id) 问题件案件数
          from claim_ods.accept_list_record a
          join claim_ods.claim c
            on a.accept_num = c.acceptance_no
           and c.delete_flag = '0'
          join claim_ods.question_claim q
            on c.claim_no = q.claim_no
         group by c.insure_company_channel,
         substr(q.gmt_created,1,10),
         a.DEPARTMENT_CODE
        )
        GROUP BY insure_company_channel, comm_date
     ),
    a12 as
     (
       SELECT
        insure_company_channel,
        comm_date,
        SUM(住院案件数) 住院案件数
       FROM
       (
        SELECT
        CASE
          WHEN c.insure_company_channel = 'CP10' THEN a.DEPARTMENT_CODE
          ELSE c.insure_company_channel
        END insure_company_channel,
        substr(a.T_CRT_TIME,1,10) comm_date,
        count(DISTINCT c.id) 住院案件数
      FROM
        claim_ods.accept_list_record a
      JOIN claim_ods.claim c
      ON a.accept_num = c.acceptance_no AND c.delete_flag = '0'
      WHERE c.delete_flag = '0'
        AND EXISTS (
          SELECT
            1
          FROM
            claim_ods.bill b
          WHERE
            b.claim_id = c.id
            AND b.delete_flag = '0'
            AND b.treatment_date IS NULL
        )
      GROUP BY c.insure_company_channel, substr(a.T_CRT_TIME,1,10),a.DEPARTMENT_CODE
       )
       GROUP BY insure_company_channel, comm_date
     ),
    a13 as
     (##审核自动化率计算： 平安用老算法，非平安的用这个新算法：
                  select insure_company_channel,
       comm_date,
        count(DISTINCT  case when C_HANDLE_CDE = '1'    then  C_CLAIM_CASE_NO else null end) num,
    count(DISTINCT    C_CLAIM_CASE_NO ) as  sh_claim_count,
        case when coalesce(COUNT(DISTINCT  C_CLAIM_CASE_NO ),0)=0 then 0
        else COUNT(DISTINCT CASE WHEN C_HANDLE_CDE = '1'  THEN C_CLAIM_CASE_NO ELSE NULL END) / COUNT(DISTINCT  C_CLAIM_CASE_NO )
        end
      as shzdhl

from  ( select
     CASE
        WHEN c.insure_company_channel = 'CP10' THEN alro.DEPARTMENT_CODE
        ELSE c.insure_company_channel
    END insure_company_channel,
     substr(cat.T_AUDIT_END_TM,1,10) comm_date,
     cat.C_HANDLE_CDE,
     cat.C_CLAIM_CASE_NO,p.product_type

FROM
    claim_ods.claim c
LEFT JOIN claim_ods.case_audit_task cat
ON cat.c_claim_case_no = c.claim_no AND cat.C_DEL_FLAG = '0'
LEFT JOIN claim_ods.ACCEPT_LIST_RECORD alro
ON c.ACCEPTANCE_NO = alro.accept_num AND alro.DEL_FLAG = '0'
        LEFT JOIN claim_ods.clm_app_info cai ON cai.C_CUSTOM_APP_NO = c.claim_no AND cai.C_DEL_FLAG = '0' AND cai.INSURANCE_COMPANY = 'CP08'
        LEFT JOIN claim_ods.claim_policy cp ON cp.policy_no = cai.C_PLY_NO
        left join claim_ods.ply p on cp.group_policy_no = p.C_PLY_NO and p.C_DEL_FLAG = '0'
         left  join   gg  on gg.app_no = c.claim_no
WHERE cat.T_AUDIT_END_TM IS NOT NULL    and  gg.app_no is null  ) ff  group by insure_company_channel,comm_date
  )
     SELECT
      DISTINCT
      insure_company_channel,
      gmt_created,
      jjl,
      cancel_vol,
      xxqd,
      xsqd,
      lrl,
      drhcal,
      blchcal,
      qlchcal,
      cast(case when coalesce(drhcal, 0) = 0 THEN 0
              when coalesce(qlchcal, 0) = 0 THEN 0
        ELSE coalesce(qlchcal, 0) / coalesce(drhcal, 0) END as DECIMAL(10,4))
            perc_all_flow,
      hcsbal,
      zdshal,
      ZDSHAL_RW,
      cast(case  when coalesce(qlchcal, 0) = 0  and  insure_company_channel<>'泰康养老广东分公司' THEN 0
          else   case   WHEN insure_company_channel='泰康养老广东分公司' and coalesce(qlchcal, 0) + coalesce(blchcal, 0) <>0 THEN coalesce(zdshal, 0) /(coalesce(qlchcal, 0) + coalesce(blchcal, 0))
                        WHEN insure_company_channel='泰康养老广东分公司' and coalesce(qlchcal, 0) + coalesce(blchcal, 0) =0 THEN 0
                ELSE coalesce(zdshal, 0) / coalesce(qlchcal, 0)  END END   as DECIMAL(10,4)) shzdhl,
        cast(case when shzdhl_rw>1 then 1 else shzdhl_rw end as DECIMAL(10,4)),
        fh_auto_claim_count,
        cast(case
        when coalesce(qlchcal, 0) = 0 THEN 0
      ELSE CASE WHEN insure_company_channel='泰康养老广东分公司' THEN
               coalesce(fh_auto_claim_count, 0) /(coalesce(qlchcal, 0)+coalesce(blchcal, 0))
                ELSE coalesce(fh_auto_claim_count, 0) / coalesce(qlchcal, 0)  END
        END as DECIMAL(10,4)) as fhzdhl,
        question_claim_vol,
        hospatal_claim_vol,
        replace(substring(gmt_created,1,10),'-','') ,
        sh_claim_count
    FROM
    (
      select DISTINCT dc_dim.channel_value insure_company_channel,
               dc_dim.date_dtd gmt_created,
               coalesce(a1.jjl, 0) jjl,
               coalesce(ca.cancel_vol, 0) cancel_vol,
               coalesce(a2.xxqd, 0) xxqd,
               coalesce(a3.xsqd, 0) xsqd,
               coalesce(a4.lrl, 0) lrl,
               coalesce(a5.drhcal, 0) drhcal,
               coalesce(a6.blchcal, 0) blchcal,
           coalesce(a7.qlchcal, 0) qlchcal,
               coalesce(a8.hcsbal, 0) hcsbal,
               coalesce(a9.zdshal, 0) zdshal,
               coalesce(a13.shzdhl, 0) as shzdhl_rw,
           coalesce(a13.num, 0) as ZDSHAL_RW,
               coalesce(a10.fh_auto_claim_count, 0) fh_auto_claim_count,
               coalesce(a11.问题件案件数, 0) question_claim_vol,
               coalesce(a12.住院案件数, 0) hospatal_claim_vol,
               coalesce(a13.sh_claim_count, 0) as sh_claim_count,
               replace(dc_dim.date_dtd,'-','')
		      from (select * from claim_ods.DIM_CHANNEL_DATE
where date_dt >=(CURRENT_DATE - INTERVAL '100' DAY)  and  date_dt <='{today}' and  CHANNEL_KEY
in  ('CP02',
'TK07',
'DJ01',
'BH01',
'ZA01',
'TK04',
'TK02',
'TK06',
'CP08','CP11',
'大连分公司',
'苏州分公司',
'TK01','GS01','CP11','TK09', 'TK10','YH01','上海分公司', 'TK12','TK11' ,'PA02','CS01')  ) dc_dim
		      left join a1   on dc_dim.date_dtd = a1.comm_date and dc_dim.channel_key = a1.insure_company_channel  and  a1.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join ca   on dc_dim.date_dtd = ca.comm_date and dc_dim.channel_key = ca.insure_company_channel   and  ca.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a2   on dc_dim.date_dtd = a2.comm_date and dc_dim.channel_key = a2.insure_company_channel  and  a2.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a3   on dc_dim.date_dtd = a3.comm_date and dc_dim.channel_key = a3.insure_company_channel   and  a3.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a4   on dc_dim.date_dtd = a4.comm_date and dc_dim.channel_key = a4.insure_company_channel  and  a4.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a5   on dc_dim.date_dtd = a5.comm_date and dc_dim.channel_key = a5.insure_company_channel  and  a5.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a6   on dc_dim.date_dtd = a6.comm_date  and dc_dim.channel_key = a6.insure_company_channel  and  a6.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a7   on dc_dim.date_dtd = a7.comm_date  and dc_dim.channel_key = a7.insure_company_channel  and  a7.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a8   on dc_dim.date_dtd = a8.comm_date and dc_dim.channel_key = a8.insure_company_channel  and  a8.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a9   on dc_dim.date_dtd = a9.comm_date  and dc_dim.channel_key = a9.insure_company_channel  and  a9.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a10  on dc_dim.date_dtd = a10.comm_date  and dc_dim.channel_key = a10.insure_company_channel  and  a10.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a11  on dc_dim.date_dtd = a11.comm_date  and dc_dim.channel_key = a11.insure_company_channel  and  a11.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a12  on dc_dim.date_dtd = a12.comm_date and dc_dim.channel_key = a12.insure_company_channel  and  a12.comm_date>=(CURRENT_DATE - INTERVAL '100' DAY)
		      left join a13  on dc_dim.date_dtd = a13.comm_date and dc_dim.channel_key = a13.insure_company_channel);


"""


def truncate_table(table_name='CLAIM_DWD.DWD_CLAIM_COUNT_DAY'):
    with DatabaseConnection() as conn:
        truncate_sql = f"delete  from  {table_name}  where  insure_company_channel not in ('平安产险_个','平安产险_团','众安暖哇_团','众安暖哇_个','平安产险_车险','平安产险_雇主','太保苏州_雇主','太保苏州_医疗','中智')  and  GMT_CREATED>=(CURRENT_DATE - INTERVAL '100' DAY) "
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
