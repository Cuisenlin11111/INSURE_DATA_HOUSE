
drop table if exists inphile_ods.ele_invoice_qrcode_result;

CREATE TABLE inphile_ods.ele_invoice_qrcode_result (
    id VARCHAR(128) NOT NULL PRIMARY KEY COMMENT '主键',
    case_id VARCHAR(50) COMMENT '案件号',
    image_id VARCHAR(50) COMMENT '影像的唯一 ID',
    image_path VARCHAR(50) COMMENT '影像的路径',
    image_url VARCHAR(50) COMMENT '影像的地址',
    qr_code_result VARCHAR(100) COMMENT '二维码识别结果',
    invoice_code VARCHAR(40) COMMENT '票据代码',
    invoice_no VARCHAR(20) COMMENT '票据号码',
    invoice_check_code VARCHAR(20) COMMENT '校验码',
    bill_date TIMESTAMP COMMENT '开票日期',
    sum_amt DECIMAL(20,6) COMMENT '金额',
    status INT COMMENT '处理状态',
    deal_type VARCHAR(1) COMMENT '处理方式;0-二维码识别；1-OCR识别；3-手动录入',
    create_by VARCHAR(32) COMMENT '创建人',
    create_time TIMESTAMP COMMENT '创建时间',
    update_by VARCHAR(32) COMMENT '更新人',
    update_time TIMESTAMP COMMENT '更新时间',
    deleted INT DEFAULT 0 COMMENT '是否删除',
    message VARCHAR(255) COMMENT '信息',
    handle_user_id VARCHAR(30) COMMENT '处理人'
)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='电票二维码识别结果';

drop table if exists inphile_ods.case_log;

CREATE TABLE inphile_ods.case_log (
    id VARCHAR(64) NOT NULL PRIMARY KEY COMMENT '主键',
    case_id VARCHAR(64) COMMENT '案件 id',
    user_id VARCHAR(64) COMMENT '操作人',
    node VARCHAR(100) COMMENT '环节',
    handle VARCHAR(100) COMMENT '操作',
    handle_result VARCHAR(100) COMMENT '操作结果',
    handle_time TIMESTAMP(6) COMMENT '操作时间',
    remark VARCHAR(500) COMMENT '备注',
    create_by VARCHAR(64) COMMENT '创建',
    create_time TIMESTAMP(6)  COMMENT '创建时间',
    update_by VARCHAR(64) COMMENT '更新',
    update_time TIMESTAMP(6) COMMENT '更新时间',
    deleted INT DEFAULT 0 COMMENT '0 未删除 1 删除'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='案件操作日志表';

drop table if exists inphile_ods.case_image;

CREATE TABLE inphile_ods.case_image (
    id VARCHAR(64) NOT NULL PRIMARY KEY COMMENT '主键',
    original_name VARCHAR(50) COMMENT '原始文件名称',
    orginal_path VARCHAR(50) COMMENT '原始文件路径',
    thumbnail_path VARCHAR(50) COMMENT '缩率图路径',
    revise_path VARCHAR(50) COMMENT '修正图文件路径',
    catgory VARCHAR(50) COMMENT '分类路径',
    image_type INTEGER COMMENT '影像类型（电子票，纸值票，明细，身份证，银行卡）',
    file_length INTEGER COMMENT '文件大小',
    recovery BOOLEAN DEFAULT FALSE COMMENT '回收站 f正常   t回收站',
    parent_id VARCHAR(64) COMMENT '影像父级',
    case_id VARCHAR(64) COMMENT '案件 id',
    height INTEGER COMMENT '高',
    width INTEGER COMMENT '宽',
    suffix VARCHAR(64) COMMENT '文件格式',
    repeat_id VARCHAR(64) COMMENT '重复影像 id',
    algorithm_task_id VARCHAR(64) COMMENT '算法任务 id',
    sort INTEGER COMMENT '排序',
    samples INTEGER DEFAULT 1 COMMENT '一张影像上有多少个样本',
    create_by VARCHAR(64) COMMENT '创建者',
    create_time TIMESTAMP(6)  COMMENT '创建时间',
    update_by VARCHAR(64) COMMENT '更新者',
    update_time TIMESTAMP(6) COMMENT '更新时间',
    deleted INTEGER DEFAULT 0 COMMENT '0 未删除 1 删除',
    original_type VARCHAR(200) COMMENT '原始分类编号(甲方的/一般重录补录的有)',
    angle INTEGER COMMENT '旋转角度 (一般顺时针 0 - 360)',
    has_detail INTEGER COMMENT '是否有明细,1 有,2 没有',
    image_url VARCHAR(50) COMMENT '查看影像地址',
    source_image_type INTEGER COMMENT '快瞳影像类型',
    core_image_type VARCHAR(10) COMMENT '回传理赔影像类型',
    remark VARCHAR(255) COMMENT '备注'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='案件影像表';


drop table if exists inphile_ods.case_bill;

CREATE TABLE inphile_ods.case_bill (
    id VARCHAR(64) NOT NULL PRIMARY KEY COMMENT '主键',
    bill_payer VARCHAR(50) COMMENT '账单姓名',
    bill_number VARCHAR(20) COMMENT '账单号',
    apply_id VARCHAR(50) COMMENT '申请 id',
    image_id VARCHAR(50) COMMENT '影像序号(账单关联的影像)',
    electronic VARCHAR(50) COMMENT '电子票导入',
    bill_type VARCHAR(10) COMMENT '账单类型',
    medical_date TIMESTAMP(6) COMMENT '就诊日期.',
    in_hospital_date TIMESTAMP(6) COMMENT '入院日期.',
    out_hospital_date TIMESTAMP(6) COMMENT '出院日期.',
    hospital_name VARCHAR(50) COMMENT '医院名称.',
    hospital_code VARCHAR(50) COMMENT '就诊医院代码',
    department_name VARCHAR(50) COMMENT '科室名称',
    department_code VARCHAR(50) COMMENT '科室代码',
    department_name_standard VARCHAR(50) COMMENT '科室名称(标准)',
    department_code_standard VARCHAR(50) COMMENT '科室代码(标准)',
    chief_diagnosis_name VARCHAR(50) COMMENT '主要诊断名称',
    chief_diagnosis_code VARCHAR(50) COMMENT '主要诊断代码',
    chief_diagnosis_name_standard VARCHAR(50) COMMENT '主要诊断名称(标准)',
    chief_diagnosis_code_standard VARCHAR(50) COMMENT '主要诊断代码(标准)',
    other_diagnosis_name VARCHAR(50) COMMENT '其它诊断名称',
    other_diagnosis_code VARCHAR(50) COMMENT '其它诊断编码',
    diagnosis_name_one VARCHAR(50) COMMENT '录入诊断 1',
    diagnosis_name_one_standard VARCHAR(50) COMMENT '录入诊断 1(标准)',
    diagnosis_name_two VARCHAR(50) COMMENT '录入诊断 2',
    diagnosis_name_two_standard VARCHAR(50) COMMENT '录入诊断 2(标准)',
    diagnosis_name_three VARCHAR(50) COMMENT '录入诊断 3',
    diagnosis_name_three_standard VARCHAR(50) COMMENT '录入诊断 3(标准)',
    special VARCHAR(20) COMMENT '是否特需',
    patient_special VARCHAR(20) COMMENT '是否门特',
    emergency_treatment VARCHAR(20) COMMENT '是否急诊',
    total_amount NUMERIC(20,2) COMMENT '账单总金额',
    social_security_payment NUMERIC(20,2) COMMENT '社保支付金额',
    classified_self_payment NUMERIC(20,2) COMMENT '分类自负金额',
    own_payment NUMERIC(20,2) COMMENT '自费金额',
    additional_payment NUMERIC(20,2) COMMENT '附加支付',
    personal_account_payment NUMERIC(20,2) COMMENT '个人账户支付',
    personal_payment NUMERIC(20,2) COMMENT '个人支付金额',
    third_pay VARCHAR(100) COMMENT '第三方支付',
    remark VARCHAR(50) COMMENT '备注',
    recovery VARCHAR(20) COMMENT '是否康复',
    diagnosis_years VARCHAR(20) COMMENT '诊断年限',
    past_history VARCHAR(200) COMMENT '既往史',
    diagnostic_source VARCHAR(200) COMMENT '诊断来源',
    diagnostic_source_num VARCHAR(100) COMMENT '来源编号',
    diagnostic_source_date TIMESTAMP(6) COMMENT '来源日期',
    bill_form VARCHAR(20) COMMENT '账单形式',
    bill_date TIMESTAMP(6) COMMENT '开票时间',
    first_self_payment NUMERIC(20,2) COMMENT '首先自付金额',
    first_self_ratio NUMERIC(20,2) COMMENT '首先自付比例',
    large_medical_assistanc_self NUMERIC(20,2) COMMENT '大额医疗救助金个人负担',
    this_year_account_payment NUMERIC(20,2) COMMENT '本年账户支付',
    all_year_account_payment NUMERIC(20,2) COMMENT '历年账户支付',
    inside_overall_planning NUMERIC(20,2) COMMENT '纳入统筹额',
    medical_insurance_fund_payment NUMERIC(20,2) COMMENT '统筹基金支付',
    large_mutual_fund_payment NUMERIC(20,2) COMMENT '大额互助资金（住院）支付',
    medical_insurance_fund_year_payment NUMERIC(20,2) COMMENT '统筹基金年度内累计支付',
    large_mutual_fund_outpatient_this_payment NUMERIC(20,2) COMMENT '大额互助资金（门诊）支付-本次支付',
    large_mutual_fund_years_total_payment NUMERIC(20,2) COMMENT '大额互助资金（门诊）支付-年度内累计支付',
    start_pay_line NUMERIC(20,2) COMMENT '起付线',
    self_pay_one_jing NUMERIC(20,2) COMMENT '自付一(京)',
    self_pay_two_jing NUMERIC(20,2) COMMENT '自付二（京）',
    self_pay_hu NUMERIC(20,2) COMMENT '自负（沪）',
    balance_before_settlement_shen NUMERIC(20,2) COMMENT '结算前余额（深）',
    balance_after_settlement_shen NUMERIC(20,2) COMMENT '结算后余额（深）',
    third_pay_amount NUMERIC(20,2) COMMENT '第三方支付金额',
    create_by VARCHAR(64) COMMENT '创建',
    create_time TIMESTAMP(6) DEFAULT NOW() COMMENT '创建时间',
    update_by VARCHAR(64) COMMENT '更新',
    update_time TIMESTAMP(6) COMMENT '更新时间',
    deleted INTEGER DEFAULT 0 COMMENT '0 未删除 1 删除',
    sort INTEGER COMMENT '排序',
    hospital_name_standard VARCHAR(50) COMMENT '医院名称(回传/标准)',
    hospital_code_standard VARCHAR(50) COMMENT '医院代码(回传/标准)',
    blurry INTEGER COMMENT '发票不清默认 0,1 是,2 不是',
    invalid_invoice INTEGER DEFAULT 2 COMMENT '是否有效,默认 2,1 无效.2 有效',
    repeat_bill VARCHAR(50) COMMENT '重复账单',
    hospitalization_type VARCHAR(10) COMMENT '住院类别',
    relation_image_id VARCHAR(50) COMMENT '关联影像 ID',
    is_insurance INTEGER COMMENT '医保接口使用情况：0-正常；1-无数据；2-接口异常'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账单层信息表';




drop table if exists inphile_ods.auth_user;

CREATE TABLE inphile_ods.auth_user (
    id VARCHAR(64) NOT NULL PRIMARY KEY COMMENT '用户登录账号',
    user_name VARCHAR(50) COMMENT '用户姓名',
    status INTEGER DEFAULT 0 COMMENT '状态 0 启用 1 禁用',
    phone VARCHAR(32) COMMENT '联系电话',
    email VARCHAR(64) COMMENT '联系邮箱',
    remark VARCHAR(200) COMMENT '备注',
    team_id VARCHAR(64) COMMENT '用户所属小组 id（team---id）',
    department_name VARCHAR(100) COMMENT '用于所属部门名称',
    create_by VARCHAR(64) COMMENT '创建',
    create_time TIMESTAMP(6) COMMENT '创建时间',
    update_by VARCHAR(64) COMMENT '更新',
    update_time TIMESTAMP(6) COMMENT '更新时间',
    deleted INTEGER DEFAULT 0 COMMENT '0 未删除 1 删除'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';

drop table if exists inphile_ods.auth_team;
CREATE TABLE inphile_ods.auth_team (
    id VARCHAR(64) NOT NULL PRIMARY KEY COMMENT '主键',
    team_name VARCHAR(100) COMMENT '组名',
    status INTEGER DEFAULT 0 COMMENT '状态 0 启用 1 禁用',
    remark VARCHAR(2000) COMMENT '备注',
    create_by VARCHAR(64) COMMENT '创建人',
    create_time TIMESTAMP(6) DEFAULT NOW() COMMENT '创建时间',
    update_by VARCHAR(64) COMMENT '更新人',
    update_time TIMESTAMP(6) COMMENT '更新时间',
    deleted INTEGER DEFAULT 0 COMMENT '0 未删除 1 删除',
    water_grade INTEGER DEFAULT 0 COMMENT '小组可以作业的水单等级,默认 0，全部，1：1 级的，2:2 级的',
    case_grade VARCHAR(255) COMMENT '小组可以作业的案件等级权限   逗号隔开',
    work_switch INTEGER COMMENT '小组获取工单的开关权限  1 开 2 关',
    low_num INTEGER COMMENT '小组的保底工作量',
    high_num INTEGER COMMENT '小组的限额量'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='团队表';


drop table if exists inphile_ods.case_info;
CREATE TABLE inphile_ods.case_info (
    id VARCHAR(64) NOT NULL PRIMARY KEY COMMENT '主键',
    ticket_id VARCHAR(32) COMMENT '票据类型 id',
    organ_id VARCHAR(64) COMMENT '机构 id',
    lot_id VARCHAR(64) COMMENT '批次号',
    status INTEGER DEFAULT 0 COMMENT '当前状态 （10 预处理 20 录入 30 质检 ）',
    handle_user_id VARCHAR(64) COMMENT '当前处理人',
    sub_status INTEGER COMMENT '子状态(status 为 20 30 时 取 auth_input 中对应节点 id)',
    current_node_time TIMESTAMP(1) COMMENT '节点开始时间',
    node_plan_end_time TIMESTAMP(1) COMMENT '节点预计结束时间',
    plan_end_time TIMESTAMP(1) COMMENT '整案预计结束时间',
    end_time TIMESTAMP(1) COMMENT '实际结束时间',
    algorithm_task_id VARCHAR(50) COMMENT '预处理分类算法任务 id',
    team_id VARCHAR(64) COMMENT '所属小组 id',
    remark VARCHAR(50) COMMENT '备注',
    create_by VARCHAR(64) COMMENT '创建者',
    create_time TIMESTAMP(6)  COMMENT '创建时间',
    update_by VARCHAR(64) COMMENT '更新者',
    update_time TIMESTAMP(6) COMMENT '更新时间',
    deleted INTEGER DEFAULT 0 COMMENT '0 未删除 1 删除',
    input_node VARCHAR(50) COMMENT ',21,22,23,',
    correct_user VARCHAR(64) COMMENT '案件批改人',
    correct_status INTEGER DEFAULT 0 COMMENT '案件批改状态 默认 0 1 批改状态',
    special_source VARCHAR(64) COMMENT '特殊件来源: 质检流转,成品文件拦截退回,甲方拦截退回',
    has_data INTEGER DEFAULT 1 COMMENT '默认 1 有数据 1 2 无数据(即无可录入的数据)',
    case_lock INTEGER DEFAULT 0 COMMENT '案件锁 默认 0 无锁定 1 锁定 ',
    lock_user VARCHAR(32) COMMENT '案件锁定人',
    case_type INTEGER COMMENT '案件类型 默认 0 正常 1 补录/重录',
    water_order INTEGER DEFAULT 1 COMMENT '默认 2,2:无水单任务 1:有水单任务',
    water_order_flag INTEGER DEFAULT 1 COMMENT '预处理标记是否发送水单，默认 1,1:发水单任务 2:不发水单任务',
    case_grade INTEGER DEFAULT 0 COMMENT '案件等级 0 无 10 一级 20 二级 30 三级 40 四级 50 五级',
    ticket_type INTEGER DEFAULT 0 COMMENT '预处提交后票据类型 1、纯电子票，2、混合票 3、纸质票：',
    department_name VARCHAR(64) COMMENT '受理机构，机构名称',
    accept_batch_no VARCHAR(64) COMMENT '受理批次号',
    is_urgent VARCHAR(4) COMMENT '是否催办 Y/N',
    urgent_lever VARCHAR(4) COMMENT '催办等级',
    urgent_reason VARCHAR(255) COMMENT '催办原因',
    time_effective NUMERIC(20,2) COMMENT '录入时效',
    collect_mode INTEGER COMMENT '录入方式(采集方式: 1 大项录入 2 医保结算大项录入 3 明细录入)',
    insure_company_channel VARCHAR(20) COMMENT '项目',
    service_line VARCHAR(20) COMMENT '业务线'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='案件信息主表';



drop table if exists inphile_ods.lot;
CREATE TABLE inphile_ods.lot (
    id VARCHAR(64) NOT NULL PRIMARY KEY COMMENT '主键',
    case_count INTEGER DEFAULT 0 COMMENT '案件数',
    back_lot VARCHAR(64) COMMENT '退回批次号',
    accept_num VARCHAR(50) COMMENT '受理编号',
    storage_path VARCHAR(50) COMMENT '原始包路径',
    base_path VARCHAR(50) COMMENT '批次基础路径',
    status INTEGER DEFAULT 0 COMMENT '0 运行中 1 失败 2 完结',
    biz_status INTEGER COMMENT '业务状态 入库中 已入库 回传中 已回传 ',
    organ_id VARCHAR(64) COMMENT '机构 id',
    remark TEXT COMMENT '备注',
    come_time TIMESTAMP(6) COMMENT '来案时间',
    plan_end_time TIMESTAMP(6) COMMENT '计划结束时间',
    end_time TIMESTAMP(6) COMMENT '最后回传时间',
    lot_date VARCHAR(50) COMMENT '批次原始日期 20210101 格式(无用)',
    create_by VARCHAR(64) COMMENT '创建者',
    create_time TIMESTAMP(6)  COMMENT '创建时间',
    update_by VARCHAR(64) COMMENT '更新者',
    update_time TIMESTAMP(6) COMMENT '更新时间',
    deleted INTEGER DEFAULT 0 COMMENT '0 未删除 1 删除',
    in_time TIMESTAMP(6) COMMENT '进件时间',
    lot_type INTEGER COMMENT '案件类型 默认 1 正常 2 补录/重录',
    insure_company_channel VARCHAR(20) COMMENT '项目',
    service_line VARCHAR(20) COMMENT '业务线'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='批次表';