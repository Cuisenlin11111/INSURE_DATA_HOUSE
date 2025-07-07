drop table if exists CLAIM_DM.ADM_PROJECT_ROFIT_M;


CREATE TABLE CLAIM_DM.ADM_PROJECT_ROFIT_M (
    insurance_company_channel VARCHAR(255) COMMENT '保险公司渠道',
    DT_MONTH VARCHAR(50) COMMENT '年月',
    DRHCAL BIGINT COMMENT '回传案件量',
    CLAIM_NUM BIGINT COMMENT '案件量',
    Case_Income DECIMAL(10,2) COMMENT '案件收入',
    Tax_Exempt_Income DECIMAL(10,2) COMMENT '除税收入',
    Marketing_Cost DECIMAL(10,2) COMMENT '营销成本',
    Gross_Profit DECIMAL(10,2) COMMENT '毛利润',
    Gross_Profit_Ratio DECIMAL(10,4) COMMENT '毛利润率',
    Gross_Profit_2 DECIMAL(10,2) COMMENT '毛利润2',
    Gross_Profit_Ratio_2 DECIMAL(10,4) COMMENT '毛利润率2',
    Sim_Profit DECIMAL(10,2) COMMENT '单纯净利润',
    Sim_Profit_Ratio DECIMAL(10,4) COMMENT '单纯净利润率',
    Net_Profit DECIMAL(10,2) COMMENT '净利润',
    Net_Profit_Ratio DECIMAL(10,4) COMMENT '净利润率',
    Entry_Cost_Ratio DECIMAL(10,4) COMMENT '外包录入占收入比',
    Entry_Cost_Case DECIMAL(10,2) COMMENT '外包录入成本案件',
    Technical_Cost_Case DECIMAL(10,2) COMMENT '项目技术成本案件',
    Operation_Cost_Case DECIMAL(10,2) COMMENT '项目作业成本案件',
    Total_Cost_Case DECIMAL(10,2) COMMENT '总成本案件',
    Personnel_Capacity_Psn  DECIMAL(10,2) COMMENT '项目作业人力',
    Personnel_Capacity DECIMAL(10,2) COMMENT '项目作业人均产能',
    Case_Avg_Price DECIMAL(10,2) COMMENT '案件均价',
    Case_Avg_Price_1 DECIMAL(10,2) COMMENT '案件均价不含撤案',
    SBLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '施博录入成本占比',
    SBLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '施博录入均价',
    SBLR_NUM BIGINT NOT NULL COMMENT '施博录入案件量',
    SHLR_COST DECIMAL(10, 2) NOT NULL COMMENT '施博录入成本',
    CDSJLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '成都视觉录入成本占比',
    CDSJLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '成都视觉录入均价',
    CDSJLR_NUM BIGINT NOT NULL COMMENT '成都视觉录入案件量',
    CDSJLR_COST DECIMAL(10, 2) NOT NULL COMMENT '成都视觉录入成本',
    GNLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '广纳录入成本占比',
    GNLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '广纳录入均价',
    GNLR_NUM BIGINT NOT NULL COMMENT '广纳录入案件量',
    GNLR_COST DECIMAL(10, 2) NOT NULL COMMENT '广纳录入成本',
    ZZSJLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '智在录入成本占比',
    ZZSJLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '智在录入均价',
    ZZSJLR_NUM BIGINT NOT NULL COMMENT '智在录入案件量',
    ZZSJLR_COST DECIMAL(10, 2) NOT NULL COMMENT '智在录入成本',
    dt VARCHAR(8) COMMENT '分区时间'
)ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='项目管理公摊月统计表';

drop table if exists CLAIM_DM.ADM_PROJECT_ROFIT_M_T;

CREATE TABLE CLAIM_DM.ADM_PROJECT_ROFIT_M_T (
    insurance_company_channel VARCHAR(255) COMMENT '保险公司渠道',
    DT_MONTH VARCHAR(50) COMMENT '年月',
    DRHCAL BIGINT COMMENT '回传案件量',
    CLAIM_NUM BIGINT COMMENT '案件量',
    Case_Income DECIMAL(10,2) COMMENT '案件收入',
    Tax_Exempt_Income DECIMAL(10,2) COMMENT '除税收入',
    Marketing_Cost DECIMAL(10,2) COMMENT '营销成本',
    Gross_Profit DECIMAL(10,2) COMMENT '毛利润',
    Gross_Profit_Ratio DECIMAL(10,4) COMMENT '毛利润率',
    Gross_Profit_2 DECIMAL(10,2) COMMENT '毛利润2',
    Gross_Profit_Ratio_2 DECIMAL(10,4) COMMENT '毛利润率2',
    Sim_Profit DECIMAL(10,2) COMMENT '单纯净利润',
    Sim_Profit_Ratio DECIMAL(10,4) COMMENT '单纯净利润率',
    Net_Profit DECIMAL(10,2) COMMENT '净利润',
    Net_Profit_Ratio DECIMAL(10,4) COMMENT '净利润率',
    Entry_Cost_Ratio DECIMAL(10,4) COMMENT '外包录入占收入比',
    Entry_Cost_Case DECIMAL(10,2) COMMENT '外包录入成本案件',
    Technical_Cost_Case DECIMAL(10,2) COMMENT '项目技术成本案件',
    Operation_Cost_Case DECIMAL(10,2) COMMENT '项目作业成本案件',
    Total_Cost_Case DECIMAL(10,2) COMMENT '总成本案件',
    `HLP_MANAGE_M`  DECIMAL(10, 2)   NOT NULL COMMENT '意健TPA慧理赔管理公摊月',
    `HLP_MANAGE_CLAIM`  DECIMAL(10, 2)   NOT NULL COMMENT '意健TPA慧理赔管理公摊案件',
    `YSJ_MANAGE_M`  DECIMAL(10, 2)   NOT NULL COMMENT '意健TPA公司管理公摊月',
    `YSJ_MANAGE_CLAIM`  DECIMAL(10, 2)   NOT NULL COMMENT '意健TPA公司管理公摊案件',
    Personnel_Capacity_Psn  DECIMAL(10,2) COMMENT '项目作业人力',
    Personnel_Capacity DECIMAL(10,2) COMMENT '项目作业人均产能',
    Case_Avg_Price DECIMAL(10,2) COMMENT '案件均价',
    Case_Avg_Price_1 DECIMAL(10,2) COMMENT '案件均价不含撤案',
    SBLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '施博录入成本占比',
    SBLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '施博录入均价',
    SBLR_NUM BIGINT NOT NULL COMMENT '施博录入案件量',
    SHLR_COST DECIMAL(10, 2) NOT NULL COMMENT '施博录入成本',
    CDSJLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '成都视觉录入成本占比',
    CDSJLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '成都视觉录入均价',
    CDSJLR_NUM BIGINT NOT NULL COMMENT '成都视觉录入案件量',
    CDSJLR_COST DECIMAL(10, 2) NOT NULL COMMENT '成都视觉录入成本',
    GNLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '广纳录入成本占比',
    GNLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '广纳录入均价',
    GNLR_NUM BIGINT NOT NULL COMMENT '广纳录入案件量',
    GNLR_COST DECIMAL(10, 2) NOT NULL COMMENT '广纳录入成本',
    ZZSJLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '智在录入成本占比',
    ZZSJLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '智在录入均价',
    ZZSJLR_NUM BIGINT NOT NULL COMMENT '智在录入案件量',
    ZZSJLR_COST DECIMAL(10, 2) NOT NULL COMMENT '智在录入成本',
    dt VARCHAR(8) COMMENT '分区时间'
)ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='项目管理公摊渠道汇总月统计表';



CREATE view CLAIM_DM.VM_CLAIM_NUM_M as
select sum(CLAIM_NUM) CLAIM_NUM  from CLAIM_DWS.DWS_CLAIM_INCOME_M
 where DT_MONTH = substr(curdate(),1,7);

CREATE view CLAIM_DM.VM_CLAIM_INCOME_M as
select sum(claim_income)  claim_income from CLAIM_DWS.DWS_CLAIM_INCOME_M
 where DT_MONTH = substr(curdate(),1,7);

CREATE view CLAIM_DM.VM_PRODUCT_CAPACITY_M as
    select distinct  product_capacity  from
CLAIM_DWS.DWS_CLAIM_MANAGE_SHARE_M   where DT_MONTH = substr(curdate(),1,7);



select *
from  CLAIM_DWS.DWS_CLAIM_INCOME_M;





CREATE view CLAIM_DM.VM_WORKER_CLAIM_UNFINISHED as
select   worker 业务员,
         dic.channel_value 渠道,
         sum(case when node=7 then 1 else 0 end ) 审核待处理数量,
         sum(case when node=8 then 1 else 0 end ) 复核待处理数量
from claim_ods.claim_console_record  ccr
left   join claim_ods.dim_insure_company_channel dic on ccr.insure_company_channel = dic.channel_key
where  is_end = 0 and node <> 6 and nvl(node,'')<>''  and is_deleted = 'N'
and coalesce(worker,'')<>''
and  worker<>'系统自动'
and worker<>'KNVS'
and worker<>'YSJU'
and  worker<>'SPIC'
and  worker<>'GTRS'
and  worker<>'system'
and  worker<>'1'
and insure_company_channel = 'TK02'
group by worker
order by dic.channel_value  desc ,审核待处理数量 desc;



drop table if exists CLAIM_DM.ADM_PRO_ROFIT_M_NEW;

CREATE TABLE CLAIM_DM.ADM_PRO_ROFIT_M_NEW (
    insurance_company_channel VARCHAR(255) COMMENT '保险公司渠道',
    DT_MONTH VARCHAR(50) COMMENT '年月',
    DRHCAL BIGINT COMMENT '回传案件量',
    CLAIM_NUM BIGINT COMMENT '案件量',
    Case_Income DECIMAL(10,2) COMMENT '案件收入',
    Tax_Exempt_Income DECIMAL(10,2) COMMENT '除税收入',
    Marketing_Cost DECIMAL(10,2) COMMENT '营销成本',
    Gross_Profit DECIMAL(10,2) COMMENT '毛利润',
    Gross_Profit_Ratio DECIMAL(10,4) COMMENT '毛利润率',
    Net_Profit DECIMAL(10,2) COMMENT '净利润',
    Net_Profit_Ratio DECIMAL(10,4) COMMENT '净利润率',
    Entry_Cost_Ratio DECIMAL(10,4) COMMENT '外包录入占收入比',
    Entry_Cost_Case DECIMAL(10,2) COMMENT '外包录入成本案件',
    Technical_Cost_Case DECIMAL(10,2) COMMENT '项目技术成本案件',
    Operation_Cost_Case DECIMAL(10,2) COMMENT '项目作业成本案件',
    Operation_Cost_Ratio DECIMAL(10,4) COMMENT   '项目作业成本占比',
    Total_Cost_Case DECIMAL(10,2) COMMENT '总成本案件',
    Personnel_Capacity_Psn  DECIMAL(10,2) COMMENT '项目作业人力',
    Personnel_Capacity DECIMAL(10,2) COMMENT '项目作业人均产能',
    Case_Avg_Price DECIMAL(10,2) COMMENT '案件均价',
    Case_Avg_Price_1 DECIMAL(10,2) COMMENT '案件均价不含撤案',
    SBLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '施博录入成本占比',
    SBLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '施博录入均价',
    SBLR_NUM BIGINT NOT NULL COMMENT '施博录入案件量',
    SHLR_COST DECIMAL(10, 2) NOT NULL COMMENT '施博录入成本',
    CDSJLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '成都视觉录入成本占比',
    CDSJLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '成都视觉录入均价',
    CDSJLR_NUM BIGINT NOT NULL COMMENT '成都视觉录入案件量',
    CDSJLR_COST DECIMAL(10, 2) NOT NULL COMMENT '成都视觉录入成本',
    GNLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '广纳录入成本占比',
    GNLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '广纳录入均价',
    GNLR_NUM BIGINT NOT NULL COMMENT '广纳录入案件量',
    GNLR_COST DECIMAL(10, 2) NOT NULL COMMENT '广纳录入成本',
    YSJLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '因朔桔录入成本占比',
    YSJLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '因朔桔录入均价',
    YSJLR_NUM BIGINT NOT NULL COMMENT '因朔桔录入案件量',
    YSJLR_COST DECIMAL(10, 2) NOT NULL COMMENT '因朔桔录入成本',
    dt VARCHAR(8) COMMENT '分区时间'
)ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='项目管理公摊月统计表NEW';

drop table if exists CLAIM_DM.ADM_PRO_ROFIT_M_T_NEW;

CREATE TABLE CLAIM_DM.ADM_PRO_ROFIT_M_T_NEW (
    insurance_company_channel VARCHAR(255) COMMENT '保险公司渠道',
    DT_MONTH VARCHAR(50) COMMENT '年月',
    DRHCAL BIGINT COMMENT '回传案件量',
    CLAIM_NUM BIGINT COMMENT '案件量',
    Case_Income DECIMAL(10,2) COMMENT '案件收入',
    Tax_Exempt_Income DECIMAL(10,2) COMMENT '除税收入',
    Marketing_Cost DECIMAL(10,2) COMMENT '营销成本',
    Gross_Profit DECIMAL(10,2) COMMENT '毛利润',
    Gross_Profit_Ratio DECIMAL(10,4) COMMENT '毛利润率',
    Net_Profit DECIMAL(10,2) COMMENT '净利润',
    Net_Profit_Ratio DECIMAL(10,4) COMMENT '净利润率',
    Entry_Cost_Ratio DECIMAL(10,4) COMMENT '外包录入占收入比',
    Entry_Cost_Case DECIMAL(10,2) COMMENT '外包录入成本案件',
    Technical_Cost_Case DECIMAL(10,2) COMMENT '项目技术成本案件',
    Operation_Cost_Case DECIMAL(10,2) COMMENT '项目作业成本案件',
    Operation_Cost_Ratio DECIMAL(10,4) COMMENT   '项目作业成本占比',
    Total_Cost_Case DECIMAL(10,2) COMMENT '总成本案件',
    `HLP_MANAGE_M`  DECIMAL(10, 2)   NOT NULL COMMENT '意健TPA慧理赔管理公摊月',
    `HLP_MANAGE_CLAIM`  DECIMAL(10, 2)   NOT NULL COMMENT '意健TPA慧理赔管理公摊案件',
    `YSJ_MANAGE_M`  DECIMAL(10, 2)   NOT NULL COMMENT '意健TPA公司管理公摊月',
    `YSJ_MANAGE_CLAIM`  DECIMAL(10, 2)   NOT NULL COMMENT '意健TPA公司管理公摊案件',
    Personnel_Capacity_Psn  DECIMAL(10,2) COMMENT '项目作业人力',
    Personnel_Capacity DECIMAL(10,2) COMMENT '项目作业人均产能',
    Case_Avg_Price DECIMAL(10,2) COMMENT '案件均价',
    Case_Avg_Price_1 DECIMAL(10,2) COMMENT '案件均价不含撤案',
    SBLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '施博录入成本占比',
    SBLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '施博录入均价',
    SBLR_NUM BIGINT NOT NULL COMMENT '施博录入案件量',
    SHLR_COST DECIMAL(10, 2) NOT NULL COMMENT '施博录入成本',
    CDSJLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '成都视觉录入成本占比',
    CDSJLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '成都视觉录入均价',
    CDSJLR_NUM BIGINT NOT NULL COMMENT '成都视觉录入案件量',
    CDSJLR_COST DECIMAL(10, 2) NOT NULL COMMENT '成都视觉录入成本',
    GNLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '广纳录入成本占比',
    GNLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '广纳录入均价',
    GNLR_NUM BIGINT NOT NULL COMMENT '广纳录入案件量',
    GNLR_COST DECIMAL(10, 2) NOT NULL COMMENT '广纳录入成本',
    YSJLR_RATE DECIMAL(10, 4) NOT NULL COMMENT '因朔桔录入成本占比',
    YSJLR_AVG_Price DECIMAL(10, 2) NOT NULL COMMENT '因朔桔录入均价',
    YSJLR_NUM BIGINT NOT NULL COMMENT '因朔桔录入案件量',
    YSJLR_COST DECIMAL(10, 2) NOT NULL COMMENT '因朔桔录入成本',
    dt VARCHAR(8) COMMENT '分区时间'
)ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='项目管理公摊渠道汇总月统计表NEW';


