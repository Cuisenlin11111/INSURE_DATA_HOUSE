import pymysql

# 源数据库连接配置
source_host = 'rr-uf636kzrob17osr7j.mysql.rds.aliyuncs.com'
source_port = 3306
source_user = 'prd_readonly'
source_password = '7MmY^nEJ3fQhysj=B'
source_db_name = 'claim_prd'

# 目标数据库连接配置
target_host = 'am-uf61afo16ust6f600167320.ads.aliyuncs.com'
target_port = 3306
target_user = 'claim_all'
target_password = 'S#5DH1ar%*1n'
target_db_name = 'claim_ods'

# 每次批量插入的记录数量，可根据实际情况调整
batch_size = 1000

# 创建源数据库连接
source_connection = pymysql.connect(host=source_host,
                                     port=source_port,
                                     user=source_user,
                                     password=source_password,
                                     db=source_db_name,
                                     charset='utf8mb4')
try:
    # 创建游标用于执行源数据库查询
    source_cursor = source_connection.cursor(pymysql.cursors.DictCursor)


    # 执行查询语句，这里使用正确的三引号或者f字符串方式来构建查询语句
    query_sql = """
    select
-- 案件号
        alr.ACCEPT_NUM  案件号,
-- 结算编号
    alr.case_no 结算编号,
-- 案件类型
    alr.apply_claim_mode 案件类型,

    CASE  alr.apply_claim_mode
        WHEN 3 then '一站式结算'
        WHEN 4 THEN '中心零报-本地就医'
        WHEN 5 THEN '中心零报-省内异地'
        WHEN 6 THEN '中心零报-跨省就医'
        WHEN 7 THEN '本地就医'
        WHEN 8 THEN '省内异地'
        WHEN 9 THEN '跨省就医'
        ELSE '未知类型'
    END AS 案件类型名称,

-- 出险人
    alr.danger_name 出险人,
-- 出险人证件类型
    alr.danger_id_type 出险人证件类型,
-- 出险人证件号
    alr.danger_id_no 出险人证件号,
     CASE
        WHEN  alr.danger_id_type = '1' AND LENGTH( alr.danger_id_no) = 18 THEN
            TIMESTAMPDIFF(YEAR,
                STR_TO_DATE(SUBSTR(alr.danger_id_no, 7, 8), '%Y%m%d'),
                CURDATE()) +1
            -
            CASE
                WHEN DATE_FORMAT(CURDATE(), '%m%d') < DATE_FORMAT(STR_TO_DATE(SUBSTR(alr.danger_id_no, 7, 8), '%Y%m%d'), '%%m%d')
                THEN 1
                ELSE 0
            END
        ELSE NULL
    END AS age ,
CASE
    WHEN LENGTH(alr.danger_id_no) = 18
    THEN CASE
           WHEN SUBSTRING(alr.danger_id_no, 17, 1) % 2 = 1 THEN '男'
           ELSE '女'
         END
    ELSE ''
END AS 性别,

    DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.begndate') / 1000), '%Y-%m-%d') AS 开始时间,
     DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.enddate') / 1000), '%Y-%m-%d') AS 结束时间,
     DATE_FORMAT(FROM_UNIXTIME(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.setlTime') / 1000), '%Y-%m-%d') 结算时间,
     JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mdtrtInfo.fixmedinsName') 医院名称,

     JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mdtrtInfo.dscgDiseName') 诊断名称,
     JSON_EXTRACT(alr.extra, '$.ybClaimRequest.medType') 医疗类别,
     JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mdtrtInfo.insutype') 险种,

-- 总费用
  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.medfeeSumamt') 总费用,
-- 全自费
    JSON_EXTRACT(alr.extra, '$.ybClaimRequest.ownpayAmt') 全自费,
-- 乙类先自付
     JSON_EXTRACT(alr.extra, '$.ybClaimRequest.preselfpayAmt') 乙类先自付,
-- 超限额自付
     JSON_EXTRACT(alr.extra, '$.ybClaimRequest.overlmtSelfpay') 超限额自付,
-- 统筹基金自付
     JSON_EXTRACT(alr.extra, '$.ybClaimRequest.hifpPay') 统筹基金自付,
-- 公务员补助
    JSON_EXTRACT(alr.extra, '$.ybClaimRequest.cvlservPay') 公务员补助,
-- 大额基金支付
    JSON_EXTRACT(alr.extra, '$.ybClaimRequest.hifobPay') 大额基金支付,
-- 大病保险支付
    JSON_EXTRACT(alr.extra, '$.ybClaimRequest.hifmiPay') 大病保险支付,
-- 医疗救助支付
    JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mafPay') 医疗救助支付,
-- 其他基金支付
    JSON_EXTRACT(alr.extra, '$.ybClaimRequest.othfundPay') 其他基金支付,

-- 一站式统筹内赔付
   case when   JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1' or alr.apply_claim_mode  in ('4','5','6')  then  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaInPay') else 0 end 一站式统筹内赔付,
-- 一站式统筹外赔付
   case when   JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1' or alr.apply_claim_mode  in ('4','5','6')  then  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaOutPay')   else 0 end 一站式统筹外赔付,
-- 一站式统筹内起付线
   case when   JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1' or alr.apply_claim_mode  in ('4','5','6')  then    JSON_EXTRACT(alr.extra, '$.ybClaimRequest.dedcInStd')  else 0 end 一站式统筹内起付线,
-- 一站式统筹外起付线
  case when   JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1' or alr.apply_claim_mode  in ('4','5','6')  then   JSON_EXTRACT(alr.extra, '$.ybClaimRequest.dedcOutStd')   else 0 end   一站式统筹外起付线,
-- 一站式最终赔付金额
   # case when   JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1'  or alr.apply_claim_mode  in ('4','5','6')  then  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')    else 0 end   一站式最终赔付金额,
    case when   JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1'  or alr.apply_claim_mode  in ('4','5','6')   and coalesce(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt'),0) <> 0
       then  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
       when  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1'  or alr.apply_claim_mode  in ('4','5','6')   and coalesce(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt'),0) = 0
       then cai.N_FINAL_COMPENSATE_AMT
       else 0 end   一站式最终赔付金额,
 JSON_EXTRACT(alr.extra, '$.ybClaimRequest.insuOptins') 参保地区,

  case when alr.apply_claim_mode  in ('4','5','6')  then 0 else  v.N_DEDUCTLE_AMT end 统筹内起付线,
  case when alr.apply_claim_mode  in ('4','5','6')  then 0 else  v.N_FINAL_PAY end  统筹内赔付,
  case when alr.apply_claim_mode  in ('4','5','6')  then 0 else v2.N_DEDUCTLE_AMT end 统筹外起付线,
  case when alr.apply_claim_mode  in ('4','5','6')  then 0 else v2.N_FINAL_PAY end 统筹外赔付,
c.clm_process_status 案件状态,JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mdtrtInfo.mdtrtId') 就诊号,
case  when  cpd.app_no IS  NULL  then '未支付'  else  '已支付'  end  支付状态,c.is_hang_up,pr.back_time 回传时间, cpd.pay_amt  保司回传金额,
    (
        CASE
            WHEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1' OR alr.apply_claim_mode IN ('4','5','6') AND COALESCE(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt'),0) <> 0
                THEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt')
            WHEN JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1' OR alr.apply_claim_mode IN ('4','5','6') AND COALESCE(JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt'),0) = 0
                THEN cai.N_FINAL_COMPENSATE_AMT
            ELSE 0
        END
    ) +
   coalesce( (CASE WHEN alr.apply_claim_mode IN ('4','5','6') THEN 0 ELSE v.N_FINAL_PAY END),0) +
   coalesce( (CASE WHEN alr.apply_claim_mode IN ('4','5','6') THEN 0 ELSE v2.N_FINAL_PAY END),0)  AS 最终赔付,        case when  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1' then '是'  else  '否'  end  一站式标识,
           case when  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1'  then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaInPay')
  else  sum(case when inv.C_RESPONSE_DESC='社保内医疗' then  inv.N_FINAL_PAY else 0 end)  end  政策内金额,
  case when  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.oneStpFlag')='1'   then JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaOutPay')
  else  sum(case when inv.C_RESPONSE_DESC='社保外医疗' then  inv.N_FINAL_PAY else 0 end ) end   政策外金额,
        case when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mdtrtInfo.insutype')='390' then '居民'
            when JSON_EXTRACT(alr.extra, '$.ybClaimRequest.mdtrtInfo.insutype')='310' then '城镇' else '' end   城镇居民标识,
        substr(cpd.pay_time,1,10) 支付时间
from claim_prd.accept_list_record alr
left join claim_prd.claim c on c.acceptance_no = alr.ACCEPT_NUM
left join claim_prd.clm_visit_info v on v.C_CUSTOM_APP_NO = c.claim_no and v.C_DEL_FLAG = '0' and v.C_RESPONSE_NO = 'SBNYL'
left join claim_prd.clm_visit_info v2 on v2.C_CUSTOM_APP_NO = c.claim_no and v2.C_DEL_FLAG = '0' and v2.C_RESPONSE_NO = 'SBWYL'
          left join claim_prd.clm_app_info cai  on  cai.C_CUSTOM_APP_NO = c.claim_no and cai.INSURANCE_COMPANY='YH01' and cai.C_DEL_FLAG='0'
          LEFT JOIN claim_prd.claim_policy cp on cp.policy_no = cai.C_PLY_NO
          LEFT JOIN claim_prd.claim_policy_customer cpc on cpc.customer_no = cp.customer_no
    LEFT JOIN
    claim_prd.postback_record pr ON c.claim_no = pr.app_no AND pr.insure_company_channel = 'YH01' AND pr.is_deleted = 'N'    and pr.receiver='I'
LEFT JOIN
    claim_prd.claim_pay_detail cpd ON cpd.app_no = pr.app_no AND cpd.is_deleted = 'N' AND cpd.pay_status = '1'
    LEFT JOIN claim_prd.clm_visit_inv_info inv
            ON inv.C_CUSTOM_APP_NO = cai.C_CUSTOM_APP_NO AND inv.C_PLY_NO = cai.C_PLY_NO AND inv.c_del_flag = '0' AND inv.C_BILL_TYP <> '3' AND inv.INSURANCE_COMPANY='YH01'

where alr.INSURE_COMPANY_CHANNEL = 'YH01' and alr.DEL_FLAG = '0' and alr.apply_claim_mode is not null
 and alr.ACCEPT_STATUS <> '5'   AND   c.clm_process_status <> '11'
and  (cai.N_FINAL_COMPENSATE_AMT>0  or  JSON_EXTRACT(alr.extra, '$.ybClaimRequest.settClaSumamt') >0 )
and   substr(pr.back_time,1,10)<=DATE_SUB(CURDATE(), INTERVAL 1 DAY)
and  pr.back_status in  ('2','21')
    group by alr.ACCEPT_NUM
    """
    source_cursor.execute(query_sql)

    # 创建目标数据库连接
    target_connection = pymysql.connect(host=target_host,
                                        port=target_port,
                                        user=target_user,
                                        password=target_password,
                                        db=target_db_name,
                                        charset='utf8mb4')
    try:
        # 创建游标用于执行目标数据库插入操作
        target_cursor = target_connection.cursor()
        trucate_sql = "TRUNCATE TABLE claim_dwd.DWD_YH01_MEDICAL_CLAIM_DATA"
        target_cursor.execute(trucate_sql)
        # 插入语句，假设表结构一致，这里用列名占位，根据实际表结构调整列名顺序
        insert_sql = f"""
        INSERT INTO claim_dwd.DWD_YH01_MEDICAL_CLAIM_DATA (
    accept_num, case_no, apply_claim_mode, case_type_name, danger_name, 
    danger_id_type, danger_id_no, age, gender, begin_date, 
    end_date,setlTime ,hospital_name, diagnosis_name, medical_type, insurance_type, 
    total_fee, full_self_pay, class_b_self_pay, over_limit_self_pay, pooling_fund_pay, 
    civil_servant_subsidy, large_amount_fund_pay, critical_illness_insurance_pay, 
    medical_assistance_pay, other_fund_pay, one_stop_pooling_inside_pay, 
    one_stop_pooling_outside_pay, one_stop_pooling_inside_deductible, 
    one_stop_pooling_outside_deductible, one_stop_final_compensation, 
    insured_area, pooling_inside_deductible, pooling_inside_compensation, 
    pooling_outside_deductible, pooling_outside_compensation, case_status, 
    visit_number, payment_status, is_hang_up, back_time, 
    insurance_company_back_amount, final_pay,    one_step_ind ,
    zcw_pay ,
    zcn_pay ,
    juming_ind ,
    pay_time 
) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""



        # 用于存储批量插入的数据
        data_to_insert = []
        for row in source_cursor:
            # 将每行数据转换为对应的值元组，这里需要根据实际表结构来准确提取值
            values = tuple(row.values())
            data_to_insert.append(values)
            if len(data_to_insert) >= batch_size:
                # 批量执行插入操作
                target_cursor.executemany(insert_sql, data_to_insert)
                data_to_insert = []
                target_connection.commit()

        # 处理剩余不足一批的数据
        if data_to_insert:
            target_cursor.executemany(insert_sql, data_to_insert)
            target_connection.commit()

    except Exception as e:
        print(f"插入目标库时出错: {e}")
        target_connection.rollback()
    finally:
        # 关闭目标数据库游标和连接
        target_cursor.close()
        target_connection.close()
except Exception as e:
    print(f"从源库查询数据时出错: {e}")
finally:
    # 关闭源数据库游标和连接
    source_cursor.close()
    source_connection.close()