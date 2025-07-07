
CREATE TABLE `CLAIM_DWS`.`DWS_CLAIM_COUNT_MONTH` (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) COMMENT '渠道', -- 保险公司的渠道信息
  `YEAR_MONTH` VARCHAR(10) COMMENT '年月', -- 年份和月份的组合，例如：202309
  `JJL` BIGINT COMMENT '进件量', -- 提交的保险案件数量
  `CANCEL_VOL` BIGINT COMMENT '撤销量', -- 撤销的保险案件数量
  `XXQD` BIGINT COMMENT '线下渠道', -- 线下渠道的保险案件数量或相关指标
  `XSQD` BIGINT COMMENT '线上渠道', -- 线上渠道的保险案件数量或相关指标
  `LRL` BIGINT COMMENT '录入量', -- 录入系统的保险案件数量
  `DRHCAL` BIGINT COMMENT '当日回传案件量', -- 当日回传的保险案件数量
  `BLCHCAL` BIGINT COMMENT '半流程回传案件量', -- 半流程回传的保险案件数量
  `QLCHCAL` BIGINT COMMENT '全流程回传案件量', -- 全流程回传的保险案件数量
  `HCSBAL` BIGINT COMMENT '回传失败案件量', -- 回传失败的保险案件数量
  `ZDSHAL` BIGINT COMMENT '自动审核案件数', -- 经过自动审核的保险案件数量
  `FH_AUTO_CLAIM_COUNT` BIGINT COMMENT '复核自动案件数', -- 经过复核自动处理的保险案件数量
  `QUESTION_CLAIM_VOL` BIGINT COMMENT '问题件案件数', -- 问题件或需要人工介入的保险案件数量
  `HOSPATAL_CLAIM_VOL` BIGINT COMMENT '住院案件数', -- 住院相关的保险案件数量
  `DAY_CN` INT COMMENT '当月实际发生天数', -- 当月实际的天数，可能用于计算平均值等
  `AVG_JJL` DECIMAL(10, 2) COMMENT '平均进件量',
  `FH_AUTO_CLAIM_RATE` DECIMAL(10, 4) COMMENT '复核自动案件率',
  `QLCHCAL_RATE` DECIMAL(10, 4) COMMENT '全流程回传案件率',
  `ZDSHAL_RATE` DECIMAL(10, 4) COMMENT '自动审核案件率(回传)',
  SHZDHL_RW    DECIMAL(10, 4) COMMENT '自动审核案件率(任务)',
  `lst_load_tm`   DATETIME COMMENT '最后一次更新时间',
  `DATA_DT` VARCHAR(10) COMMENT '调度日期' -- 调度的日期，可能是数据加载或处理的日期
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='案件数量统计月表';


CREATE TABLE `CLAIM_DWS`.`DWS_CLAIM_COUNT_WEEK` (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) COMMENT '渠道',                -- 保险公司渠道信息
  `COMM_DATE` DATE COMMENT '日期',                                    -- 具体的日期
  `WEEK_REGION` VARCHAR(30) COMMENT '周区间',                         -- 周的区间或描述
  `JJL` BIGINT COMMENT '进件量',                                    -- 提交的保险案件数量
  `CANCEL_VOL` BIGINT COMMENT '撤销量',                               -- 撤销的保险案件数量
  `XXQD` BIGINT COMMENT '线下渠道',                                   -- 线下渠道的保险案件数量或相关指标
  `XSQD` BIGINT COMMENT '线上渠道',                                   -- 线上渠道的保险案件数量或相关指标
  `LRL` BIGINT COMMENT '录入量',                                       -- 录入系统的保险案件数量
  `DRHCAL` BIGINT COMMENT '当日回传案件量',                         -- 当日回传的保险案件数量
  `BLCHCAL` BIGINT COMMENT '半流程回传案件量',                      -- 半流程回传的保险案件数量
  `QLCHCAL` BIGINT COMMENT '全流程回传案件量',                      -- 全流程回传的保险案件数量
  `HCSBAL` BIGINT COMMENT '回传失败案件量',                         -- 回传失败的保险案件数量
  `ZDSHAL` BIGINT COMMENT '自动审核案件数',                         -- 自动审核的保险案件数量
  `FH_AUTO_CLAIM_COUNT` BIGINT COMMENT '复核自动案件数',             -- 复核自动处理的保险案件数量
  `QUESTION_CLAIM_VOL` BIGINT COMMENT '问题件案件数',               -- 问题件或需要人工介入的保险案件数量
  `HOSPATAL_CLAIM_VOL` BIGINT COMMENT '住院案件数',
  `FH_AUTO_CLAIM_RATE` DECIMAL(10, 4) COMMENT '复核自动案件率',
  `QLCHCAL_RATE` DECIMAL(10, 4) COMMENT '全流程回传案件率',
  `ZDSHAL_RATE` DECIMAL(10, 4) COMMENT '自动审核案件率(回传)',
   SHZDHL_RW    DECIMAL(10, 4) COMMENT '自动审核案件率(任务)',
  `lst_load_tm`   DATETIME COMMENT '最后一次更新时间',
  `DATA_DT` VARCHAR(20) COMMENT '调度日期'                           -- 数据调度或处理的日期
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='案件数量统计周表';




CREATE TABLE `CLAIM_DWS`.`DWS_CLAIM_COUNT_DAY` (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) COMMENT '渠道',
  `GMT_CREATED` DATE COMMENT '进件日期',
  `JJL` BIGINT COMMENT '进件量',
  `CANCEL_VOL` BIGINT COMMENT '撤销量',
  `XXQD` BIGINT COMMENT '线下渠道',
  `XSQD` BIGINT COMMENT '线上渠道',
  `LRL` BIGINT COMMENT '录入量',
  `DRHCAL` BIGINT COMMENT '当日回传案件量',
  `BLCHCAL` BIGINT COMMENT '半流程回传案件量',
  `QLCHCAL` BIGINT COMMENT '全流程回传案件量',
  `PERC_ALL_FLOW` DECIMAL(10,4) COMMENT '全流程占比',
  `HCSBAL` BIGINT COMMENT '回传失败案件量',
  `ZDSHAL` BIGINT COMMENT '自动审核案件数(回传)',
  `ZDSHAL_RW` BIGINT COMMENT '自动审核案件数(任务)',
  `SHZDHL` DECIMAL(10,4) COMMENT '审核自动化率(回传)',
  `SHZDHL_RW` DECIMAL(10,4) COMMENT '审核自动化率(任务)',
  `FH_AUTO_CLAIM_COUNT` BIGINT COMMENT '复核自动案件数',
  `FHZDHL` DECIMAL(10,4) COMMENT '复核自动化率',
  `QUESTION_CLAIM_VOL` BIGINT COMMENT '问题件案件数',
  `QUESTION_CLAIM_perct` DECIMAL(10,4) COMMENT '问题件案件占比',
  `HOSPATAL_CLAIM_VOL` BIGINT COMMENT '住院案件数',
  `HOSPATAL_CLAIM_perct` DECIMAL(10,4) COMMENT '住院案件占比',
  `WEEK_QJ` VARCHAR(50) COMMENT '周区间',
  `lst_load_tm`   DATETIME COMMENT '最后一次更新时间',
  `DATA_DT` VARCHAR(20) COMMENT '调度日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='案件数量统计日表';


CREATE TABLE claim_dws.dws_project_pro_df (
    `rank` INT COMMENT '项目组排名',
    `group_name` VARCHAR(255) NOT NULL COMMENT '项目组名称',
     performance_coefficient DECIMAL(10, 2) COMMENT '绩效系数',
    `work_hours` DECIMAL(10, 2) COMMENT '已完成工时',
    `update_time` TIME(0) COMMENT '更新时间',
    dt  VARCHAR(10)  COMMENT '分区时间'
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS claim_dws.dws_employee_pro_df (
    rank INT COMMENT '排名',
    group_name VARCHAR(255) NOT NULL COMMENT '项目组名称',
    name VARCHAR(100) COMMENT '姓名',
    performance_coefficient DECIMAL(10, 2) COMMENT '绩效系数',
    work_hours DECIMAL(10, 2) COMMENT '完成工时',
    update_time TIME(0) COMMENT '更新时间',
    dt VARCHAR(10) COMMENT '分区时间'
) ENGINE=InnoDB;



CREATE TABLE `dws_employee_comp_num` (
  `business_segment` varchar(50),
  `INSURE_COMPANY_CHANNEL` varchar(50),
  `STAFF_NAME` varchar(50),
  `cnt` bigint,
  `company_name` varchar(50),
  `work_hours_factor` decimal(10, 6),
  `dt` varchar(10) COMMENT '分区时间'
) ENGINE=InnoDB;


CREATE TABLE claim_dws.dws_discharge_summary_df (
    claim_no VARCHAR(255) NOT NULL COMMENT '申请号',
    first_insure_date VARCHAR(255)  COMMENT '首次投保日期',
    image_url VARCHAR(255) COMMENT '图片地址',
    recognized_text TEXT COMMENT '识别文本信息',
    treatment_date DATE COMMENT '就诊日期',
    admission_date DATE COMMENT '入院日期',
    is_major_disease CHAR(1) COMMENT '是否重疾 (Y: 是, N: 否)',
    original_diagnosis_code VARCHAR(255) COMMENT '原疾病代码',
    original_diagnosis_name VARCHAR(255) COMMENT '原疾病名称',
    matched_diagnosis_code VARCHAR(255) COMMENT '疾病匹配代码',
    matched_diagnosis_name VARCHAR(255) COMMENT '疾病匹配名称',
    auto_diagnosis_code VARCHAR(255) COMMENT '匹配疾病CODE（系统）',
    auto_diagnosis_name VARCHAR(255) COMMENT '匹配疾病名称（系统）',
    has_medical_history CHAR(1) COMMENT '是否既往病史 (Y: 是, N: 否)',
    past_medical_history TEXT COMMENT '既往史',
    diagnosis_year VARCHAR(255) COMMENT '诊断年限',
    insurance_company_channel VARCHAR(255) COMMENT '大渠道',
    `dt` varchar(10) COMMENT '分区时间'
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='出院小结信息表';



CREATE TABLE IF NOT EXISTS claim_dws.dws_employee_pro_df_bak (
    rank INT COMMENT '排名',
    group_name VARCHAR(255) NOT NULL COMMENT '项目组名称',
    name VARCHAR(100) COMMENT '姓名',
    performance_coefficient DECIMAL(10, 2) COMMENT '绩效系数',
    work_hours DECIMAL(10, 2) COMMENT '完成工时',
    update_time TIME(0) COMMENT '更新时间',
    dt VARCHAR(10) COMMENT '分区时间'
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='业务员工时统计备份表';



CREATE TABLE claim_dws.dws_discharge_summary_encrypt_df_new (
    claim_no VARCHAR(255) NOT NULL COMMENT '申请号',
    first_insure_date VARCHAR(255)  COMMENT '首次投保日期',
    image_url VARCHAR(255) COMMENT '图片地址',
    recognized_text TEXT COMMENT '识别脱敏信息',
    treatment_date DATE COMMENT '就诊日期',
    admission_date DATE COMMENT '入院日期',
    is_major_disease CHAR(1) COMMENT '是否重疾 (Y: 是, N: 否)',
    original_diagnosis_code VARCHAR(255) COMMENT '原疾病代码',
    original_diagnosis_name VARCHAR(255) COMMENT '原疾病名称',
    matched_diagnosis_code VARCHAR(255) COMMENT '疾病匹配代码',
    matched_diagnosis_name VARCHAR(255) COMMENT '疾病匹配名称',
    auto_diagnosis_code VARCHAR(255) COMMENT '匹配疾病CODE（系统）',
    auto_diagnosis_name VARCHAR(255) COMMENT '匹配疾病名称（系统）',
    is_medical_history CHAR(1) COMMENT '是否既往病史 (Y: 是, N: 否)',
    past_medical_history TEXT COMMENT '既往史',
    diagnosis_year VARCHAR(255) COMMENT '诊断年限',
    insurance_company_channel VARCHAR(255) COMMENT '大渠道',
    `dt` varchar(10) COMMENT '分区时间'
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='出院小结脱敏信息表';



CREATE TABLE claim_dws.dws_discharge_summary_nlp_handle (
    claim_no VARCHAR(255) NOT NULL COMMENT '申请号',
    first_insure_date VARCHAR(255)  COMMENT '首次投保日期',
    image_url VARCHAR(255) COMMENT '图片地址',
    recognized_text JSON COMMENT '识别脱敏信息',
    treatment_date DATE COMMENT '就诊日期',
    admission_date DATE COMMENT '入院日期',
    is_major_disease CHAR(1) COMMENT '是否重疾 (Y: 是, N: 否)',
    original_diagnosis_code VARCHAR(255) COMMENT '原疾病代码',
    original_diagnosis_name VARCHAR(255) COMMENT '原疾病名称',
    matched_diagnosis_code VARCHAR(255) COMMENT '疾病匹配代码',
    matched_diagnosis_name VARCHAR(255) COMMENT '疾病匹配名称',
    auto_diagnosis_code VARCHAR(255) COMMENT '匹配疾病CODE（系统）',
    auto_diagnosis_name VARCHAR(255) COMMENT '匹配疾病名称（系统）',
    is_medical_history CHAR(1) COMMENT '是否既往病史 (Y: 是, N: 否)',
    past_medical_history TEXT COMMENT '既往史',
    diagnosis_year VARCHAR(255) COMMENT '诊断年限',
    insurance_company_channel VARCHAR(255) COMMENT '大渠道',
    `dt` varchar(10) COMMENT '分区时间'
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='出院小结nlp半结构表';



CREATE TABLE `CLAIM_DWS`.`DWS_ALL_FLOW_EFF_MONITOR` (
    `INSURE_COMPANY_CHANNEL` VARCHAR(50) COMMENT '渠道',
    `CREATE_TIME` DATE COMMENT '时间',
    `AUTOSH_CLAIM` bigint  COMMENT '自动审核案件数',
    `ALLSH_CLAIM` bigint  COMMENT '当天审核案件总数',
    `SHZDHL_RW` DECIMAL(10,4) COMMENT '审核自动化率(任务)',
    `SH_AUTO_RATE` DECIMAL(10,4) COMMENT '审核自动化率(回传)',
    `FH_AUTO_RATE` DECIMAL(10,4) COMMENT '复核自动化率',
    `IN_CLAIM` bigint  COMMENT '案件录入量',
    `PER_MANUAL_CLAIM_VOL` bigint  COMMENT '预审人工案件数',
    `PER_MANUAL_RATE` DECIMAL(10,4)  COMMENT '预审自动化率',
    `HOSPITAL_MANUAL_VOL` bigint  COMMENT '医院人工匹配量',
    `HOSPITAL_MATCH_TOTAL` bigint  COMMENT '医院匹配总量',
    `HOSPITAL_MATCH_RATE` DECIMAL(10,4)  COMMENT '医院匹配自动化率',
    `DIAG_MATCH_MANUAL_VOL` bigint  COMMENT '诊断匹配人工数量',
    `DIAG_MATCH_TOTAL` bigint  COMMENT '诊断匹配全量',
    `DIAG_MATCH_RATE` DECIMAL(10,4)  COMMENT '诊断匹配自动化率',
    `DETAIL_MANUAL_MATCH_VOL` bigint  COMMENT '明细人工匹配量',
    `DETAIL_TOTAL` bigint  COMMENT '明细总数量',
    `DETAIL_RATE` DECIMAL(10,4)  COMMENT '明细自动化率',
    `MANUAL_CHARGE_BILL_VOL` bigint  COMMENT '人工扣费发票量',
    `MANUAL_CHARGE_VOL` bigint  COMMENT '人工扣费案件量',
    `CHARGE_TOTAL` bigint  COMMENT '扣费总量',
    `CHARGE_RATE` DECIMAL(10,4)  COMMENT '扣费发票自动化率',
    `CONFIG_MANUAL_VOL` bigint  COMMENT '配置人工数',
    `CONFIG_TOTAL` bigint  COMMENT '配置总数',
    `CONFIG_RATE` DECIMAL(10,4) COMMENT '配置自动化率',
    `ZDGZ_TOTAL` bigint  COMMENT '诊断规则总数',
    `ZDGZ_ZD` bigint  COMMENT '自动诊断规则数',
    `ZDGZ_ZD_RATE` DECIMAL(10,4) COMMENT '诊断规则自动化率',
    `DATA_DT` VARCHAR(10) COMMENT '调度日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全流程自动化率汇总表';



CREATE TABLE `CLAIM_DWS`.`DWS_ALL_FLOW_EFF_WEEK` (
    `INSURE_COMPANY_CHANNEL` VARCHAR(50) COMMENT '渠道',
    `WEEK_REGION` VARCHAR(30) COMMENT '周区间',
    `SH_AUTO_RATE` DECIMAL(10,4) COMMENT '审核自动化率',
    `FH_AUTO_RATE` DECIMAL(10,4) COMMENT '复核自动化率',
    `PER_MANUAL_RATE` DECIMAL(10,4) COMMENT '预审自动化率',
    `HOSPITAL_MATCH_RATE` DECIMAL(10,4) COMMENT '医院自动化匹配率',
    `DIAG_MATCH_RATE` DECIMAL(10,4) COMMENT '诊断自动化匹配率',
    `DETAIL_MATCH_RATE` DECIMAL(10,4) COMMENT '明细匹配自动化率',
    `CHARGE_BILL_RATE` DECIMAL(10,4) COMMENT '扣费发票自动化率',
    `CONFIG_RATE` DECIMAL(10,4) COMMENT '配置自动化率',
    `ZDGZ_ZD_RATE` DECIMAL(10,4) COMMENT '诊断规则自动化率',
     `DATA_DT` VARCHAR(10) COMMENT '数据日期（调度日期）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='周度全流程效率指标表';


CREATE TABLE `CLAIM_DWS`.`DWS_ALL_FLOW_EFF_MONTH` (
    `INSURE_COMPANY_CHANNEL` VARCHAR(50) COMMENT '渠道',
    `YEAR_MONTH` VARCHAR(10) COMMENT '年月',
    `SH_AUTO_RATE` DECIMAL(10,4) COMMENT '审核自动化率',
    `FH_AUTO_RATE` DECIMAL(10,4) COMMENT '复核自动化率',
    `PER_MANUAL_RATE` DECIMAL(10,4) COMMENT '预审人工占比（原文为预审自动化率，根据上下文理解可能需要调整）',
    `HOSPITAL_MATCH_RATE` DECIMAL(10,4) COMMENT '医院自动化率',
    `DIAG_MATCH_RATE` DECIMAL(10,4) COMMENT '诊断自动化率',
    `DETAIL_MATCH_RATE` DECIMAL(10,4) COMMENT '明细匹配自动化率',
    `CHARGE_BILL_RATE` DECIMAL(10,4) COMMENT '扣费发票自动化率',
    `CONFIG_RATE` DECIMAL(10,4) COMMENT '配置自动化率',
    `ZDGZ_ZD_RATE` DECIMAL(10,4) COMMENT '诊断规则自动化率',
    `DATA_DT` VARCHAR(10) COMMENT '调度日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='月度全流程自动化率指标表';


CREATE TABLE CLAIM_DWS.DWS_EXAM_RISK_ANALYSIS_POLICY (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) not null COMMENT '渠道',
  `GROUP_POLICY_NO` VARCHAR(50) COMMENT '保单号',
  `COMM_DATE` DATE COMMENT '时间',
  `RISK_LAYER` VARCHAR(10) COMMENT '风控层次',
  `C_RISK_REASON` VARCHAR(1000) COMMENT '风控原因',
   `EXPLAIN_CODE` VARCHAR(1000) COMMENT '原因解释码',
  `EXPLAIN_DESC` VARCHAR(1000) COMMENT '原因解释',
  `TRIGGER_CLAIM_NUM` BIGINT  COMMENT '触发案件数',
  `CLAIM_NO` VARCHAR(30) COMMENT '申请号',
    product_type  VARCHAR(30) COMMENT '类型',
  `DATA_DT` VARCHAR(10) COMMENT '调度日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='风控分析详细数据表';




CREATE TABLE claim_dws.`dws_claim_risk_analysis_detail` (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) NOT NULL COMMENT '渠道',
  `channel_order` BIGINT NOT NULL COMMENT '排序',
  `case_status` VARCHAR(50) NOT NULL COMMENT '案件状态',
  `total_count` BIGINT NOT NULL COMMENT '总计',
  `distributed_count` BIGINT NOT NULL COMMENT '分配数',
  `pre_audit_count` BIGINT NOT NULL COMMENT '预审数',
  `match_count` BIGINT NOT NULL COMMENT '匹配数',
  `rule_match_count` BIGINT NOT NULL COMMENT '规则匹配数',
  `deduct_count` BIGINT NOT NULL COMMENT '扣费数',
  `auto_audit_count` BIGINT NOT NULL COMMENT '自动审核数',
  `audit_count` BIGINT NOT NULL COMMENT '审核数',
  `review_count` BIGINT NOT NULL COMMENT '复核数',
  `company_review_count` BIGINT NOT NULL COMMENT '保司复核数',
  `pending_count` BIGINT NOT NULL COMMENT '待处理数',
  `postback_count` BIGINT NOT NULL COMMENT '回传数',
  `entry_count` BIGINT NOT NULL COMMENT '录入数',
  `issue_count` BIGINT NOT NULL COMMENT '问题件数',
  `status_order` BIGINT NOT NULL COMMENT '状态排序',
  `channel_sort` BIGINT NOT NULL COMMENT '渠道排序'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='控制台风控分析明细表';


CREATE TABLE claim_dws.`dws_claim_risk_analysis_total` (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) NOT NULL COMMENT '渠道值',
  `pending_total` BIGINT NOT NULL COMMENT '待处理总数',
  `pending_today` BIGINT NOT NULL COMMENT '今日待处理数',
  `warning` BIGINT NOT NULL COMMENT '预警数',
  `overdue` BIGINT NOT NULL COMMENT '超时数',
  `daily_intake` BIGINT NOT NULL COMMENT '当天进件量',
  `daily_completion` BIGINT NOT NULL COMMENT '当天完成量'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='控制台风控分析汇总表';

drop  table if exists claim_dws.DWS_CLAIM_MANAGE_SHARE_M;

CREATE TABLE CLAIM_DWS.DWS_CLAIM_MANAGE_SHARE_M(
    `DT_MONTH` VARCHAR(20) NOT NULL COMMENT '年月',
    `CLAIM_NUM` bigint  NOT NULL COMMENT '案件量',
    `product_capacity`  DECIMAL(10, 2)    COMMENT '总人均产能',
    `HLP_MANAGE_M`  DECIMAL(10, 2)    COMMENT '意健TPA慧理赔管理公摊月',
    `HLP_MANAGE_CLAIM`  DECIMAL(10, 2)    COMMENT '意健TPA慧理赔管理公摊案件',
    `YSJ_MANAGE_M`  DECIMAL(10, 2)    COMMENT '意健TPA公司管理公摊月',
    `YSJ_MANAGE_CLAIM`  DECIMAL(10, 2)    COMMENT '意健TPA公司管理公摊案件',
    `dt` varchar(10) COMMENT '分区时间'
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='项目管理公摊月统计表';


CREATE TABLE claim_dws.DWS_INPUT_COST_M (
    entry_merchant VARCHAR(255) NOT NULL COMMENT '录入商',
    insurance_company_channel VARCHAR(255) COMMENT '大渠道',
   `DT_MONTH` VARCHAR(20) NOT NULL COMMENT '年月',
    `IN_CLAIM_NUM` bigint  NOT NULL COMMENT '录入案件量',
    `expense`  DECIMAL(10, 2)   NOT NULL COMMENT '录入成本',
    `avg_expense`  DECIMAL(10, 2)   NOT NULL COMMENT '成本均价',
    `dt` varchar(10) COMMENT '分区时间'
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='项目录入成本月统计表';

drop table if exists claim_dws.DWS_CLAIM_INCOME_M;

CREATE TABLE CLAIM_DWS.DWS_CLAIM_INCOME_M(
    insurance_company_channel VARCHAR(255) COMMENT '大渠道',
   `DT_MONTH` VARCHAR(20)  COMMENT '年月',
   `DRHCAL` bigint   COMMENT '回传案件量',
    `CLAIM_NUM` bigint  NOT NULL COMMENT '案件量',
    `claim_income`  DECIMAL(10, 2)   NOT NULL COMMENT '案件收入',
    `dt` varchar(10) COMMENT '分区时间'
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='项目收入月统计表';


CREATE TABLE `CLAIM_DWS`.`DWS_CLAIM_JJL_SEQ` (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) COMMENT '渠道',
  `GMT_CREATED` DATE COMMENT '进件日期',
  `JJL` BIGINT COMMENT '进件量',
   `JJL_1WEEK_AGO`  BIGINT COMMENT '一周前环比',
   `JJL_1WEEK_RATE` DECIMAL(10, 4) COMMENT '一周前环比增长率',
   `JJL_4WEEKS_AGO`  BIGINT COMMENT '四周前环比',
   `JJL_4WEEKS_RATE` DECIMAL(10, 4) COMMENT '四周前环比增长率',
  `DATA_DT` VARCHAR(10) COMMENT '调度日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='案件数量环比统计表';


CREATE TABLE CLAIM_DWS.DWS_CHANNEL_RISK_ANALYSIS_TOTAL (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) not null COMMENT '渠道',
  `COMM_DATE` DATE COMMENT '时间',
  `C_RISK_REASON` VARCHAR(1000) COMMENT '风控原因',
  `TRIGGER_CLAIM_NUM` BIGINT  COMMENT '触发案件数',
  `DATA_DT` VARCHAR(10) COMMENT '调度日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='风控分析渠道汇总';


CREATE TABLE CLAIM_DWS.DWS_POLICY_RISK_ANALYSIS_TOTAL (
  `GROUP_POLICY_NO` VARCHAR(50) COMMENT '保单号',
  `COMM_DATE` DATE COMMENT '时间',
  `C_RISK_REASON` VARCHAR(1000) COMMENT '风控原因',
  `TRIGGER_CLAIM_NUM` BIGINT  COMMENT '触发案件数',
  `DATA_DT` VARCHAR(10) COMMENT '调度日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='风控分析保单汇总';




CREATE TABLE claim_dws.dws_medical_record_summary_df (
    claim_no VARCHAR(255) NOT NULL COMMENT '申请号',
    image_url VARCHAR(255) COMMENT '图片地址',
    recognized_text TEXT COMMENT '识别文本信息',
    treatment_date DATE COMMENT '就诊日期',
    hospital_name VARCHAR(255)  COMMENT '医院名称',
    auto_diagnosis_code VARCHAR(255) COMMENT '匹配疾病CODE（系统）',
    auto_diagnosis_name VARCHAR(255) COMMENT '匹配疾病名称（系统）',
    insurance_company_channel VARCHAR(255) COMMENT '大渠道',
    `dt` varchar(10) COMMENT '分区时间'
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='超药量信息表';


CREATE TABLE claim_dws.dws_medical_record_encrypt_df (
    claim_no VARCHAR(255) NOT NULL COMMENT '申请号',
    image_url VARCHAR(255) COMMENT '图片地址',
    recognized_text TEXT COMMENT '脱敏信息',
    treatment_date DATE COMMENT '就诊日期',
     hospital_name VARCHAR(255)  COMMENT '医院名称',
    auto_diagnosis_code VARCHAR(255) COMMENT '匹配疾病CODE（系统）',
    auto_diagnosis_name VARCHAR(255) COMMENT '匹配疾病名称（系统）',
    insurance_company_channel VARCHAR(255) COMMENT '大渠道',
    `dt` varchar(10) COMMENT '分区时间'
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='超药量脱敏信息表';











CREATE TABLE `CLAIM_DWS`.`DWS_CLAIM_RETURN_CT` (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) COMMENT '渠道', -- 保险公司渠道信息
  `COMM_TIME` DATE COMMENT '日期', -- 具体的日期
  `RETURN_AUDIT` BIGINT COMMENT '退回审核', -- 退回审核的案件数量
  `RETURN_CHARGE` BIGINT COMMENT '退回扣费', -- 退回扣费的案件数量
  `RETURN_ENTER` BIGINT COMMENT '退回录入', -- 退回录入的案件数量
  `HCBS_THSH` BIGINT COMMENT '回传保司退回审核', -- 回传保司后退回审核的案件数量
  `BSFH_THSH` BIGINT COMMENT '保司复核退回审核', -- 保司复核后退回审核的案件数量
  `DRHCAL` BIGINT COMMENT '回传案件量', -- 回传的案件数量
  `CCL` DECIMAL(10, 4) COMMENT '保司退回差错率', -- 保司退回的差错率
  `DATA_DT` VARCHAR(20) COMMENT '调度日期' -- 调度的日期，可能是数据加载或处理的日期
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='案件退回差错率统计表';










CREATE TABLE `CLAIM_DWS`.`DWS_BISIC_DATA_PANEL` (
  `INSURE_COMPANY_CHANNEL` VARCHAR(50) COMMENT '渠道',
  `CREATE_TIME` DATE COMMENT '时间',
  `TOTAL_CLAIM_COUNT` BIGINT COMMENT '受理案件量',
  `TOTAL_MZ_CLAIM_COUNT` BIGINT COMMENT '门诊案件量',
  `HOSPITAL_CLAIM_COUNT` BIGINT COMMENT '住院案件数',
  `MZ_ONLINE_CLAIM_COUNT` BIGINT COMMENT '门诊线上案件数',
  `MZ_OFFLINE_CLAIM_COUNT` BIGINT COMMENT '门诊线下案件数',
  `ALL_FLOW_CLAIM_COUNT` BIGINT COMMENT '全流程案件量',
  `HALF_FLOW_CLAIM_COUNT` BIGINT COMMENT '半流程案件量',
  `TOTAL_HALF_ALL_FLOW` BIGINT COMMENT '全流程和半流程案件量之和',
  `TOTAL_BILL_COUNT` BIGINT COMMENT '总发票数',
  `MZ_BILL_COUNT` BIGINT COMMENT '门诊发票数',
  `ELEC_BILL_VOL` BIGINT COMMENT '电子发票数量',
  `ZY_BILL_COUNT` BIGINT COMMENT '住院发票数',
  `TOTAL_DETAIL_COUNT` BIGINT COMMENT '总明细条数',
  `MZ_DETAIL_COUNT` BIGINT COMMENT '门诊明细条数',
  `ZY_BILL_DETAIL_COUNT` BIGINT COMMENT '住院发票明细数',
  `ELEC_BILL_CLAIM_VOL` BIGINT COMMENT '电子发票案件数',
  `ALL_ELEC_BILL_CLAIM_VOL` BIGINT COMMENT '全案电子发票案件数',
  `DATA_DT` VARCHAR(20) COMMENT '调度日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='基础数据看板';





CREATE TABLE `CLAIM_DWS`.`DWS_OPERATION_PLAN_INFO` (
    channel VARCHAR(255) COMMENT '渠道',
    year VARCHAR(4) COMMENT '年份',
    indicator VARCHAR(255) COMMENT '指标',
    january DECIMAL(10, 2) COMMENT '一月',
    february DECIMAL(10, 2) COMMENT '二月',
    march DECIMAL(10, 2) COMMENT '三月',
    april DECIMAL(10, 2) COMMENT '四月',
    may DECIMAL(10, 2) COMMENT '五月',
    june DECIMAL(10, 2) COMMENT '六月',
    july DECIMAL(10, 2) COMMENT '七月',
    august DECIMAL(10, 2) COMMENT '八月',
    september DECIMAL(10, 2) COMMENT '九月',
    october DECIMAL(10, 2) COMMENT '十月',
    november DECIMAL(10, 2) COMMENT '十一月',
    december DECIMAL(10, 2) COMMENT '十二月'
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='运营计划表';