-- 创建一个名为 medical_consumables 的表，用于存储医用耗材相关信息
CREATE TABLE `CLAIM_ODS`.medical_consumables_t (
    medical_insurance_generic_name VARCHAR(255) NOT NULL COMMENT '医保通用名',
    first_level_category VARCHAR(255) COMMENT '一级分类 (学科、品类)',
    second_level_category VARCHAR(255) COMMENT '二级分类 (用途、品目)',
    third_level_category VARCHAR(255) COMMENT '三级分类 (部位、功能、品种)',
    consumable_material VARCHAR(255) COMMENT '耗材材质',
    specification VARCHAR(255) COMMENT '规格 (特征、参数)',
    consumable_enterprise VARCHAR(255) COMMENT '耗材企业',
    medical_consumable_code VARCHAR(255) PRIMARY KEY COMMENT '医用耗材代码'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='耗材主数据2';








CREATE TABLE `CLAIM_ODS`.base_hospital_v4 (
    business_nature VARCHAR(255) COMMENT '经营性质',
    number VARCHAR(255) COMMENT '编号',
    medical_insurance_zone VARCHAR(255) COMMENT '医保区划',
    medical_institution_grade VARCHAR(255) COMMENT '医疗机构等级',
    designated_medical_institution_name VARCHAR(255) COMMENT '定点医疗机构名称',
    address VARCHAR(255) COMMENT '地址',
    unified_social_credit_code VARCHAR(255) COMMENT '统一社会信用代码',
    designated_medical_institution_code VARCHAR(255) COMMENT '定点医疗机构代码',
    medical_institution_type VARCHAR(255) COMMENT '医疗机构类型',
    credit_rating VARCHAR(255) COMMENT '信用等级'
);


CREATE TABLE `CLAIM_ODS`.base_consumable_v4 (
    material_code VARCHAR(255) COMMENT '材料代码',
    material_name VARCHAR(255) COMMENT '材料名称',
    claim_type VARCHAR(255) COMMENT '出险类型',
    category_1 VARCHAR(255) COMMENT '分类1',
    category_2 VARCHAR(255) COMMENT '分类2',
    category_3 VARCHAR(255) COMMENT '分类3',
    category_4 VARCHAR(255) COMMENT '分类4',
    major_item VARCHAR(255) COMMENT '大项',
    sub_item VARCHAR(255) COMMENT '次项',
    indication VARCHAR(255) COMMENT '适应症',
    age_limit VARCHAR(255) COMMENT '年龄限制',
    gender_limit VARCHAR(255) COMMENT '性别限制',
    first_level VARCHAR(255) COMMENT '一级（学科、品类）',
    second_level VARCHAR(255) COMMENT '二级（用途、品目）',
    third_level VARCHAR(255) COMMENT '三级（部位、功能、品种）'
);



==============================================================



广东医保三目录：


CREATE TABLE `CLAIM_ODS`.base_guangdong_consumable (
    id INT  COMMENT '序号',
    medical_supplies_code VARCHAR(255) COMMENT '医用耗材代码',
    first_level_category VARCHAR(255) COMMENT '一级分类',
    second_level_category VARCHAR(255) COMMENT '二级分类',
    third_level_category VARCHAR(255) COMMENT '三级分类',
    remarks VARCHAR(255) COMMENT '备注',
    basic_medical_insurance_type VARCHAR(255) COMMENT '基础医保类型',
    basic_medical_insurance VARCHAR(255)  COMMENT '基本医疗保险',
    maternity_insurance VARCHAR(255)  COMMENT '生育保险',
    work_injury_insurance VARCHAR(255)  COMMENT '工伤保险'
) COMMENT '广东耗材目录';



