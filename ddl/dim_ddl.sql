CREATE TABLE `CLAIM_ODS`.`DIM_CHANNEL_DATE` (
    `CHANNEL_KEY` VARCHAR(50) COMMENT '渠道code',
    `CHANNEL_VALUE` VARCHAR(50) COMMENT '渠道value',
    `DATE_DT` VARCHAR(10) COMMENT '年月日',
    `DATE_DTD` DATE COMMENT '年月日',
    `YEAR_MONTH` VARCHAR(10) COMMENT '年月',
    `DT_YEAR` VARCHAR(6) COMMENT '年',
    `DT_MONTH` VARCHAR(6) COMMENT '月',
    `DT_DAY` VARCHAR(6) COMMENT '日',
    `DT_WEEK` VARCHAR(10) COMMENT '星期',
    `DT_JD` VARCHAR(4) COMMENT '季度',
    `BOURSE_WEEK` VARCHAR(6) COMMENT '当年第几周',
    `WEEK_QJ` VARCHAR(30) COMMENT '周区间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='渠道日期维表';




CREATE TABLE CLAIM_ODS.`DIM_DATE_WEEK` (
  `DATE_DT` VARCHAR(10) COMMENT '年月日', -- 日期字符串格式，例如：20230913
  `DATE_DTD` DATE COMMENT '年月日（日期格式）', -- 实际的日期格式
  `DT_YEAR` VARCHAR(4) COMMENT '年', -- 年份，例如：2023
  `DT_MONTH` VARCHAR(2) COMMENT '月', -- 月份，例如：09
  `DT_DAY` VARCHAR(2) COMMENT '日', -- 日期，例如：13
  `DT_WEEK` VARCHAR(1) COMMENT '星期', -- 星期几（数字或缩写），例如：3代表周三，或者MON代表周一
  `DT_JD` VARCHAR(1) COMMENT '季度', -- 季度编号，例如：3代表第三季度
  `BOURSE_WEEK` VARCHAR(2) COMMENT '当年第几周', -- 当年中的周数，例如：37代表当年的第37周
  `WEEK_QJ` VARCHAR(30) COMMENT '周区间' -- 周的区间描述，可能是一个范围或具体描述
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='日期维度表（按周）';



CREATE TABLE claim_ods.dim_project_info (
  `project_name` varchar(100) DEFAULT NULL COMMENT '项目名称',
  `code` varchar(20) DEFAULT NULL COMMENT '项目代码',
  `business_segment` int(11) NOT NULL COMMENT '业务环节编号',
  `work_hours_factor` decimal(10,6) DEFAULT NULL COMMENT '工时系数'
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4;


INSERT INTO claim_ods.dim_project_info (project_name, code, business_segment, work_hours_factor)
VALUES
  ('太保产险宁波分公司','CP02','1','0.025'),
('苏州分公司','cp10','1','0.0625'),
('大连分公司','cp10','1','0.0625'),
('北京分公司','cp08','1','0.0285714285714286'),
('上海分公司','cp08','1','0.0285714285714286'),
('太保上分','cp07','1','0.0666666666666667'),
('宁波普惠','YX01','1','0.0285714285714286'),
('平安产险','PA02','1','0.0333333333333333'),
('泰康养老广东分公司','TK07','1','0.0285714285714286'),
('泰康养老北京分公司','TK04','1','0.0181818181818182'),
('泰康养老上海分公司','TK01','1','0.0222222222222222'),
('泰康养老河南分公司','TK06','1','0.02'),
('泰康养老山东分公司','TK02','1','0.025'),
('中智','CP01','1','0.0285714285714286'),
('大家养老','DJ01','1','0.05'),
('众安暖哇_个','ZA01','1','0.16'),
('众安暖哇_团','ZA01','1','0.04'),
('渤海人寿','BH01','1','0.04'),
('太保产险宁波分公司','CP02','2','0.0181818181818182'),
('北京分公司','cp08','2','0.0181818181818182'),
('上海分公司','cp08','2','0.0181818181818182'),
('太保上分','cp07','2','0.0181818181818182'),
('宁波普惠','YX01','2','0.0181818181818182'),
('平安产险','PA02','2','0.0166666666666667'),
('泰康养老广东分公司','TK07','2','0.0208333333333333'),
('泰康养老北京分公司','TK04','2','0.0125'),
('泰康养老上海分公司','TK01','2','0.0153846153846154'),
('泰康养老河南分公司','TK06','2','0.0125'),
('泰康养老山东分公司','TK02','2','0.0166666666666667'),
('中智','CP01','2','0.0181818181818182'),
('众安暖哇_个','ZA01','2','0.1'),
('众安暖哇_团','ZA01','2','0.025'),
('渤海人寿','BH01','2','0.0181818181818182'),
('太保产险宁波分公司','CP02','3','0.0333333333333333'),
('苏州分公司','cp10','3','0.0333333333333333'),
('大连分公司','cp10','3','0.0333333333333333'),
('北京分公司','cp08','3','0.0357142857142857'),
('上海分公司','cp08','3','0.0357142857142857'),
('太保上分','cp07','3','0.0333333333333333'),
('宁波普惠','YX01','3','0.0333333333333333'),
('平安产险','PA02','3','0.05'),
('泰康养老广东分公司','TK07','3','0.0666666666666667'),
('泰康养老北京分公司','TK04','3','0.0333333333333333'),
('泰康养老上海分公司','TK01','3','0.0333333333333333'),
('泰康养老河南分公司','TK06','3','0.0333333333333333'),
('泰康养老山东分公司','TK02','3','0.0333333333333333'),
('中智','CP01','3','0.0357142857142857'),
('大家养老','DJ01','3','0.04'),
('众安暖哇_个','ZA01','3','0.1'),
('众安暖哇_团','ZA01','3','0.05'),
('渤海人寿','BH01','3','0.0333333333333333'),
('苏州分公司','cp10','4','0.0285714285714286'),
('大连分公司','cp10','4','0.0285714285714286'),
('北京分公司','cp08','4','0.0285714285714286'),
('上海分公司','cp08','4','0.0285714285714286'),
('中智','CP01','4','0.0285714285714286'),
('大家养老','DJ01','4','0.0285714285714286'),
('太保产险宁波分公司','CP02','5','0.025'),
('苏州分公司','cp10','5','0.025'),
('大连分公司','cp10','5','0.025'),
('北京分公司','cp08','5','0.025'),
('上海分公司','cp08','5','0.025'),
('太保上分','cp07','5','0.025'),
('宁波普惠','YX01','5','0.025'),
('平安产险','PA02','5','0.0555555555555556'),
('泰康养老广东分公司','TK07','5','0.0555555555555556'),
('泰康养老北京分公司','TK04','5','0.025'),
('泰康养老上海分公司','TK01','5','0.025'),
('泰康养老河南分公司','TK06','5','0.0555555555555556'),
('泰康养老山东分公司','TK02','5','0.025'),
('中智','CP01','5','0.025'),
('大家养老','DJ01','5','0.02'),
('众安暖哇_个','ZA01','5','0.025'),
('众安暖哇_团','ZA01','5','0.025'),
('渤海人寿','BH01','5','0.025'),
('太保产险宁波分公司','CP02','6','0.0166666666666667'),
('苏州分公司','cp10','6','0.0166666666666667'),
('大连分公司','cp10','6','0.0166666666666667'),
('北京分公司','cp08','6','0.0166666666666667'),
('上海分公司','cp08','6','0.0166666666666667'),
('太保上分','cp07','6','0.0166666666666667'),
('宁波普惠','YX01','6','0.0166666666666667'),
('平安产险','PA02','6','0.0166666666666667'),
('泰康养老广东分公司','TK07','6','0.0166666666666667'),
('泰康养老北京分公司','TK04','6','0.0166666666666667'),
('泰康养老上海分公司','TK01','6','0.0166666666666667'),
('泰康养老河南分公司','TK06','6','0.0166666666666667'),
('泰康养老山东分公司','TK02','6','0.0166666666666667'),
('中智','CP01','6','0.0166666666666667'),
('大家养老','DJ01','6','0.0166666666666667'),
('众安暖哇_个','ZA01','6','0.0166666666666667'),
('众安暖哇_团','ZA01','6','0.0166666666666667'),
('渤海人寿','BH01','6','0.0166666666666667'),
('太保产险宁波分公司','CP02','7','0.008'),
('苏州分公司','cp10','7','0.008'),
('大连分公司','cp10','7','0.008'),
('北京分公司','cp08','7','0.008'),
('上海分公司','cp08','7','0.008'),
('太保上分','cp07','7','0.008'),
('宁波普惠','YX01','7','0.008'),
('平安产险','PA02','7','0.008'),
('泰康养老广东分公司','TK07','7','0.008'),
('泰康养老北京分公司','TK04','7','0.008'),
('泰康养老上海分公司','TK01','7','0.008'),
('泰康养老河南分公司','TK06','7','0.008'),
('泰康养老山东分公司','TK02','7','0.008'),
('中智','CP01','7','0.008'),
('大家养老','DJ01','7','0.008'),
('众安暖哇_个','ZA01','7','0.008'),
('众安暖哇_团','ZA01','7','0.008'),
('渤海人寿','BH01','7','0.008'),
('太保产险宁波分公司','CP02','8','0.00222222222222222'),
('苏州分公司','cp10','8','0.00222222222222222'),
('大连分公司','cp10','8','0.00222222222222222'),
('北京分公司','cp08','8','0.00222222222222222'),
('上海分公司','cp08','8','0.00222222222222222'),
('太保上分','cp07','8','0.00222222222222222'),
('宁波普惠','YX01','8','0.00222222222222222'),
('平安产险','PA02','8','0.00222222222222222'),
('泰康养老广东分公司','TK07','8','0.00222222222222222'),
('泰康养老北京分公司','TK04','8','0.00222222222222222'),
('泰康养老上海分公司','TK01','8','0.00222222222222222'),
('泰康养老河南分公司','TK06','8','0.00222222222222222'),
('泰康养老山东分公司','TK02','8','0.00222222222222222'),
('中智','CP01','8','0.00222222222222222'),
('大家养老','DJ01','8','0.00222222222222222'),
('众安暖哇_个','ZA01','8','0.00222222222222222'),
('众安暖哇_团','ZA01','8','0.00222222222222222'),
('渤海人寿','BH01','8','0.00222222222222222'),
('太保产险宁波分公司','CP02','9','0.008'),
('苏州分公司','cp10','9','0.008'),
('大连分公司','cp10','9','0.008'),
('北京分公司','cp08','9','0.008'),
('上海分公司','cp08','9','0.008'),
('太保上分','cp07','9','0.008'),
('宁波普惠','YX01','9','0.008'),
('平安产险','PA02','9','0.008'),
('泰康养老广东分公司','TK07','9','0.008'),
('泰康养老北京分公司','TK04','9','0.008'),
('泰康养老上海分公司','TK01','9','0.008'),
('泰康养老河南分公司','TK06','9','0.008'),
('泰康养老山东分公司','TK02','9','0.008'),
('中智','CP01','9','0.008'),
('大家养老','DJ01','9','0.008'),
('众安暖哇_个','ZA01','9','0.008'),
('众安暖哇_团','ZA01','9','0.008'),
('渤海人寿','BH01','9','0.008'),
('太保产险宁波分公司','CP02','10','0.00277777777777778'),
('苏州分公司','cp10','10','0.00277777777777778'),
('大连分公司','cp10','10','0.00277777777777778'),
('北京分公司','cp08','10','0.00277777777777778'),
('上海分公司','cp08','10','0.00277777777777778'),
('太保上分','cp07','10','0.00277777777777778'),
('宁波普惠','YX01','10','0.00277777777777778'),
('平安产险','PA02','10','0.00277777777777778'),
('泰康养老广东分公司','TK07','10','0.00277777777777778'),
('泰康养老北京分公司','TK04','10','0.00277777777777778'),
('泰康养老上海分公司','TK01','10','0.00277777777777778'),
('泰康养老河南分公司','TK06','10','0.00277777777777778'),
('泰康养老山东分公司','TK02','10','0.00277777777777778'),
('中智','CP01','10','0.00277777777777778'),
('大家养老','DJ01','10','0.00277777777777778'),
('众安暖哇_个','ZA01','10','0.00277777777777778'),
('众安暖哇_团','ZA01','10','0.00277777777777778'),
('渤海人寿','BH01','10','0.00277777777777778');

INSERT INTO claim_ods.dim_project_info (project_name, code, business_segment, work_hours_factor)
VALUES
  ('中国人寿财产保险','GS01','1','0.067'),
   ('中国人寿财产保险','GS01','2','0.028'),
    ('中国人寿财产保险','GS01','3','0.033'),
     ('中国人寿财产保险','GS01','4','0.040'),
      ('中国人寿财产保险','GS01','5','0.056');




INSERT INTO claim_ods.dim_project_info (project_name, code, business_segment, work_hours_factor)
VALUES
  ('湖南医惠保','YH01','1','0.067'),
   ('湖南医惠保','YH01','2','0.05'),
    ('湖南医惠保','YH01','3','0.067'),
     ('湖南医惠保','YH01','4','0.0285'),
      ('湖南医惠保','YH01','5','0.0125'),
        ('湖南医惠保','YH01','6','0.022222'),
   ('湖南医惠保','YH01','7','0.008000'),
    ('湖南医惠保','YH01','8','0.002222'),
     ('湖南医惠保','YH01','9','0.008000'),
      ('湖南医惠保','YH01','10','0.002778');



INSERT INTO claim_ods.dim_project_info (project_name, code, business_segment, work_hours_factor)
VALUES
  ('湖南医惠保','YH01','1','0.067'),
   ('湖南医惠保','YH01','2','0.05'),
    ('湖南医惠保','YH01','3','0.067'),
     ('湖南医惠保','YH01','4','0.0285'),
      ('湖南医惠保','YH01','5','0.0125'),
        ('湖南医惠保','YH01','6','0.022222'),
   ('湖南医惠保','YH01','7','0.008000'),
    ('湖南医惠保','YH01','8','0.002222'),
     ('湖南医惠保','YH01','9','0.008000'),
      ('湖南医惠保','YH01','10','0.002778');

INSERT INTO claim_ods.dim_project_info (project_name, code, business_segment, work_hours_factor)
VALUES
        ('泰康养老甘肃分公司','TK11','6','0.022222'),
   ('泰康养老甘肃分公司','TK11','7','0.008000'),
    ('泰康养老甘肃分公司','TK11','8','0.002222'),
     ('泰康养老甘肃分公司','TK11','9','0.008000'),
      ('泰康养老甘肃分公司','TK11','10','0.002778');



INSERT INTO claim_ods.dim_project_info (project_name, code, business_segment, work_hours_factor)
VALUES
        ('长生人寿','CS01','6','0.022222'),
   ('长生人寿','CS01','7','0.008000'),
    ('长生人寿','CS01','8','0.002222'),
     ('长生人寿','CS01','9','0.008000'),
      ('长生人寿','CS01','10','0.002778');


CREATE TABLE claim_ods.dim_insure_company_channel (
  channel_key varchar(50) DEFAULT NULL COMMENT  '渠道key',
  channel_value varchar(50) DEFAULT NULL COMMENT '渠道值'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('TK08','泰康养老厦门分公司'),
('TK07','泰康养老广东分公司'),
('CP01','中智'),
('CP02','太保产险宁波分公司'),
('CP05','太保产险浙江分公司'),
('CP06','太保产险浙江分公司二期'),
('TK01','泰康养老上海分公司'),
('TK02','泰康养老山东分公司'),
('TK03','泰康养老浙江分公司'),
('TK04','泰康养老北京分公司'),
('TK05','泰康养老重庆分公司'),
('TK06','泰康养老河南分公司'),
('PI01','人保健康'),
('ZA01','暖哇科技'),
('YT01','蓝云保-亚太财'),
('YT03','CDP-亚太财'),
('PA01','平安保险'),
('PA02','平安产险'),
('HI01','现代财产险'),
('DJ01','大家养老'),
('YX01','宁波普惠'),
('BH01','渤海人寿');


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('CP07','太保上分');

INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('CP08','太保健康');

INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('CP10','太保财产险');


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('RB01','人保财');



INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('GS01','中国人寿财产保险');


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('TK09','泰康养老江苏分公司');


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('TK10','泰康养老辽宁分公司');


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('TK11','泰康养老甘肃分公司');


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('YH01','湖南医惠保');


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('CP11','高端医疗');



INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('TK12','泰康养老福建分公司');


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('CS01','长生人寿');


INSERT INTO claim_ods.dim_insure_company_channel (channel_key,channel_value)
VALUES
    ('上海分公司','太保产险上海分公司');


update claim_ods.dim_project_info  set work_hours_factor='0.076923' where  business_segment='1' and project_name='苏州分公司';

update claim_ods.dim_project_info  set work_hours_factor='0.037037' where  business_segment='1' and project_name='泰康养老广东分公司';

update claim_ods.dim_project_info  set work_hours_factor='0.018181' where  business_segment='2' and project_name='渤海人寿';



CREATE TABLE CLAIM_DIM.DIM_INSURE_COMPANY_CHANNEL_S (
    `CHANNEL_KEY` VARCHAR(30) COMMENT '渠道key',
    `CHANNEL_VALUE` VARCHAR(100) COMMENT '渠道value',
    `CHANNEL_STATUS` VARCHAR(100) COMMENT '状态',
    `CHANNEL_ORDER` VARCHAR(100) COMMENT '排序'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='渠道状态维表'
;






CREATE TABLE CLAIM_DIM.DIM_EFFECT_INSURE_CHANNEL (
    `CHANNEL_KEY` VARCHAR(30) COMMENT '渠道key',
    `CHANNEL_VALUE` VARCHAR(100) COMMENT '渠道value',
    `dt` VARCHAR(100) COMMENT '创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='有效渠道名称维表'
;

insert into CLAIM_DIM.DIM_EFFECT_INSURE_CHANNEL ( CHANNEL_KEY,CHANNEL_VALUE ,dt)
    VALUES
('CP08','太保健康','2024-05-15'),
('HI01','现代财产险','2024-05-15'),
('ZA01','暖哇科技','2024-05-15'),
('CP05','太保产险浙江分公司','2024-05-15'),
('YX01','宁波普惠','2024-05-15'),
('PA01','平安保险','2024-05-15'),
('PA02','平安产险','2024-05-15'),
('CP01','中智','2024-05-15'),
('CP02','太保产险宁波分公司','2024-05-15'),
('TK02','泰康养老山东分公司','2024-05-15'),
('BH01','渤海人寿','2024-05-15'),
('TK06','泰康养老河南分公司','2024-05-15'),
('TK01','泰康养老上海分公司','2024-05-15'),
('DJ01','大家养老','2024-05-15'),
('TK04','泰康养老北京分公司','2024-05-15'),
('TK07','泰康养老广东分公司','2024-05-15'),
('TK08','泰康养老厦门分公司','2024-05-15'),
('YT01','蓝云保-亚太财','2024-05-15'),
('CP10','太保-财产险','2024-05-15');

insert into CLAIM_DIM.DIM_EFFECT_INSURE_CHANNEL ( CHANNEL_KEY,CHANNEL_VALUE ,dt)
    VALUES
('GS01','','2024-05-15'),
('TK09','泰康江苏','2024-05-15');

CREATE TABLE `CLAIM_DIM`.`DIM_MANPOWER_CONFIG` (
  `DT_MONTH` VARCHAR(20) NOT NULL COMMENT '年月',
  `CHANNEL_KEY` VARCHAR(20) NOT NULL COMMENT '渠道key',
  `CHANNEL_VALUE` VARCHAR(50) NOT NULL COMMENT '渠道值',
  `CONFIG_VOL` DECIMAL(10,2) NOT NULL COMMENT '配置人力',
  `CONFIG_MONEY` BIGINT NOT NULL COMMENT '配置人力月工资',
   `CUSTOMER_VOL` DECIMAL(10,2) NOT NULL COMMENT '客服人力',
  `CUSTOMER_MONEY` DECIMAL(10,2) NOT NULL COMMENT '客服人力月工资',
  `CLAIM_OPERATE_VOL` DECIMAL(10,2) NOT NULL COMMENT '理赔运营经理人力',
  `CLAIM_OPERATE_MONEY`  BIGINT NOT NULL COMMENT '理赔运营经理人力月工资',
  `CLAIM_STANDARD_VOL` DECIMAL(10,2) NOT NULL COMMENT '理赔标准化人力',
  `CLAIM_STANDARD_MONEY` DECIMAL(10,2) NOT NULL COMMENT '理赔标准化人力月工资',
  `CLAIM_AUDIT_VOL` DECIMAL(10,2) NOT NULL COMMENT '理赔审核人力',
  `CLAIM_AUDIT_MONEY` DECIMAL(10,2) NOT NULL COMMENT '理赔审核人力月工资',
  `TECH_VOL` DECIMAL(10,2) NOT NULL COMMENT '技术人力',
  `TECH_MONEY` DECIMAL(10,2) NOT NULL COMMENT '技术人力月工资'
)
COMMENT='运营科技维度表'
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4;


CREATE TABLE claim_dim.dim_insure_company_channel (
  channel_key varchar(50) DEFAULT NULL COMMENT  '渠道key',
    dt_month varchar(50) DEFAULT NULL COMMENT '年月',
  channel_value varchar(50) DEFAULT NULL COMMENT '渠道值'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='渠道月份表';


INSERT INTO claim_dim.dim_insure_company_channel (channel_key,dt_month,channel_value)
VALUES
('TK07','2025-06','泰康养老广东分公司'),
('CP01','2025-06','中智'),
('CP02','2025-06','太保产险宁波分公司'),
('TK01','2025-06','泰康养老上海分公司'),
('TK02','2025-06','泰康养老山东分公司'),
('TK03','2025-06','泰康养老浙江分公司'),
('TK04','2025-06','泰康养老北京分公司'),
('TK06','2025-06','泰康养老河南分公司'),
('ZA01','2025-06','暖哇科技'),
('PA02','2025-06','平安产险'),
('DJ01','2025-06','大家养老'),
('YX01','2025-06','宁波普惠'),
('BH01','2025-06','渤海人寿'),
('CP08','2025-06','太保健康');


INSERT INTO claim_dim.dim_insure_company_channel (channel_key,dt_month,channel_value)
    VALUES
('苏州分公司','2025-06','太保产险苏州分公司'),
('苏州分公司','2025-05','太保产险苏州分公司'),
('苏州分公司','2025-04','太保产险苏州分公司'),
('苏州分公司','2025-03','太保产险苏州分公司'),
('苏州分公司','2025-02','太保产险苏州分公司'),
('苏州分公司','2025-01','太保产险苏州分公司'),
('苏州分公司','2024-06','太保产险苏州分公司'),
('苏州分公司','2024-07','太保产险苏州分公司'),
('苏州分公司','2024-08','太保产险苏州分公司'),
('苏州分公司','2024-09','太保产险苏州分公司'),
('苏州分公司','2024-10','太保产险苏州分公司'),
('苏州分公司','2024-11','太保产险苏州分公司'),
('苏州分公司','2024-12','太保产险苏州分公司');





INSERT INTO claim_dim.dim_insure_company_channel (channel_key,dt_month,channel_value)
    VALUES
('CP07','2025-06','太保产险上海分公司'),
('CP07','2025-05','太保产险上海分公司'),
('CP07','2025-04','太保产险上海分公司'),
('CP07','2025-03','太保产险上海分公司'),
('CP07','2025-02','太保产险上海分公司'),
('CP07','2025-01','太保产险上海分公司'),
('CP07','2024-06','太保产险上海分公司'),
('CP07','2024-07','太保产险上海分公司'),
('CP07','2024-08','太保产险上海分公司'),
('CP07','2024-09','太保产险上海分公司'),
('CP07','2024-10','太保产险上海分公司'),
('CP07','2024-11','太保产险上海分公司'),
('CP07','2024-12','太保产险上海分公司');





INSERT INTO claim_dim.dim_insure_company_channel (channel_key,dt_month,channel_value)
    VALUES
('大连分公司','2025-06','太保产险大连分公司'),
('大连分公司','2025-05','太保产险大连分公司'),
('大连分公司','2025-04','太保产险大连分公司'),
('大连分公司','2025-03','太保产险大连分公司'),
('大连分公司','2025-02','太保产险大连分公司'),
('大连分公司','2025-01','太保产险大连分公司'),
('大连分公司','2024-06','太保产险大连分公司'),
('大连分公司','2024-07','太保产险大连分公司'),
('大连分公司','2024-08','太保产险大连分公司'),
('大连分公司','2024-09','太保产险大连分公司'),
('大连分公司','2024-10','太保产险大连分公司'),
('大连分公司','2024-11','太保产险大连分公司'),
('大连分公司','2024-12','太保产险大连分公司');


CREATE TABLE `CLAIM_DIM`.`DIM_CHANNEL_PRICE` (
    `CHANNEL_KEY` VARCHAR(20) NOT NULL COMMENT '渠道代码',
    `CHANNEL_VALUE` VARCHAR(50) NOT NULL COMMENT '渠道名称',
    `CASE_UNIT_PRICE` DECIMAL(10, 2) DEFAULT 0 COMMENT '案件单价',
    `STANDARD_UNIT_PRICE` DECIMAL(10, 2) DEFAULT 0 COMMENT '标准单价',
    `PO_UNIT_PRICE` DECIMAL(10, 2) DEFAULT 0 COMMENT '个或门/险种类型单价',
    `GI_UNIT_PRICE` DECIMAL(10, 2) DEFAULT 0 COMMENT '团或住（大项）/险种类型单价',
    `INVOICE_UNIT_PRICE` DECIMAL(10, 2) DEFAULT 0 COMMENT '发票单价',
    `C_INVOICE_UNIT_PRICE` DECIMAL(10, 2) DEFAULT 0 COMMENT '超发票单价',
    `Q_UNIT_PRICE` DECIMAL(10, 2) DEFAULT 0 COMMENT '团或住（全明细）/险种类型单价'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='渠道价格表';





CREATE TABLE CLAIM_DIM.DIM_PA02_CUSTOM_PLY_NO (
    `CUSTOM_PLY_NO` VARCHAR(50) COMMENT '产品码值',
    `CUSTOM_PLY_NAME` VARCHAR(50) COMMENT '产品名称'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='平安车险产品代码表'
;


INSERT INTO CLAIM_DIM.DIM_PA02_CUSTOM_PLY_NO (CUSTOM_PLY_NO,CUSTOM_PLY_NAME)
    VALUES
        ('MP03000961', '平安车主尊享保障'),
('MP02000071', '平安非机动车保险1.0'),
('MP03001192', '平安百万车家保定制版2022款'),
('MP03200583', '平安全车驾乘险2022款(升级尊享版)'),
('MP18040405', '平安美团单车(互联网)'),
('MP03001218', '平安驾乘无忧四代保险'),
('MP03000834', '平安百万车家保精英基础版二'),
('MP03001321', '平安百万车家保2023款'),
('MP01000002', '机动车交通事故责任强制保险(2020版)'),
('MP03041218', '平安保游天下跑跑卡丁车运动保障（互联网）'),
('MP03001313', '平安百万车家保2.0'),
('MP03250690', '平安百万车家保2023款(定制)'),
('MP03210154', '平安贵州全车驾乘险2022款'),
('MP01000001', '机动车辆商业保险(2020版)'),
('MP03001194', '平安百万车家保2022款'),
('MP02210021', '平安非机动车第三者责任及驾驶人意外保险(非营运)'),
('MP03010911', '平安摩托车驾驶人意外险(车主版)'),
('MP03200528', '平安百万车家保2022款(湖南)'),
('MP02260006', '平安非机动车保险（非两轮家用）'),
('MP03210191', '平安贵州百万车家保专属版2023款'),
('MP03250741', '平安百万车家保2023款(线客专属)'),
('MP03011311', '平安百万车家保2022款(北京)'),
('MP03250577', '平安货车驾乘意外保险(安徽)'),
('MP03051789', '平安骑行非机动车保险(互联网版)'),
('MP03051530', '平安自驾游保险E款升级版(互联网版)'),
('MP03022154', '平安悦享逸家·出行险(大车代版)'),
('MP07000612', '非机动车三者责任保险'),
('MP03450202', '平安百万车家保2023款(HYS)'),
('MP03140305', '平安百万车家保2022款(广西)'),
('MP18040324', '平安代驾司机意外险(快马互联网)'),
('MP03080086', '平安车家保宝基础版'),
('MP03250386', '平安货车驾乘保险(互联网)'),
('MP03250366', '平安希望保嗨自驾境内旅行保障'),
('MP18000068', '平安江泰自驾游方案3(互联网版)'),
('MP03021655', '平安百万车家保(上海升级版)'),
('MP03250605', '平安全车驾乘险升级版(蚌埠定制T)'),
('MP02410089', '平安电单车保险(佛山)2023升级版'),
('MP03200516', '平安代驾人员意外保险'),
('MP03022130', '平安非机动车骑行保险(互联网版)'),
('MP03261233', '平安摩托车驾驶人意外险(南充专属版)'),
('MP03250425', '平安驾乘意外保险(HX)'),
('MP03021519', '平安银行好车主信用卡权益意外险(2021款)'),
('MP02000043', '平安非机动车保险'),
('MP03510217', '平安全车驾乘险2022款TS'),
('MP03100461', '平安百万车家保2023款(南京MINI)'),
('MP03200702', '平安湖南驾乘保'),
('MP03210192', '平安贵州百万车家保定制版2023款'),
('MP03250712', '平安百万车家保2023款(合肥代理专属)'),
('MP03041642', '平安保游乘风破浪水上运动保险(互联网)'),
('MP03250689', '平安驾乘人员意外险互联网(记名版)'),
('MP03001590', '平安逸家·0免赔百万医(车主尊享版)(互联网版)'),
('MP02260125', '平安非机动车第三者责任保险(四川)'),
('MP18040325', '平安代驾司机意外险(快马100)(互联网)'),
('MP03200348', '平安车E宝(五座版)'),
('MP03011115', '平安全车驾乘保险(特斯拉专属)'),
('MP03250814', '平安百万车家保2024款(安徽)'),
('MP03250830', '平安驾乘险雇员互联网版(单座)'),
('MP03000842', '平安百万车家保王牌基础版二'),
('MP03090478', '平安驾乘无忧四代保险(2022湖北版)'),
('MP02290054', '平安快递专用(宁波)非机动车保险'),
('MP03021967', '平安摩托车意外保险(互联网版SH)'),
('MP03021971', '平安中交驾驶人意外保险(含安心保)(互联网版)'),
('MP03340100', '平安驾乘安心无忧保'),
('MP03011281', '平安驾驶人意外保险(首汽非营业升级版)'),
('MP02100187', '平安非机动车保险升级版(健康无忧赠险)'),
('MP03051712', '平安驾乘无忧四代保险(代步车)'),
('MP03011327', '平安出行险升级(北京车主版)'),
('MP03250694', '平安境内短期旅游险(卡丁车)'),
('MP02040362', '平安非机动车保险(YZ升级版)（互联网）'),
('MP02000315', '平安电瓶车意外险(互联网版)'),
('MP03250765', '平安驾乘险(升级互联网版)'),
('MP03041395', '平安驾乘无忧三代保险(代步车版)'),
('MP02210043', '平安非机动车第三者及车上人员责任保险(非营业)'),
('MP18020550', '平安上海市网络预约出租汽车乘客意外伤害保险升级款'),
('MP18050618', '平安-货拉拉随车人员意外'),
('MP03210111', '平安摩托车&电动车驾驶人意外伤害保险'),
('MP18110035', '平安新驾培意外险21版(互联网版)'),
('MP03250547', '平安百万车家保2022款(996)'),
('MP03200407', '平安摩托车驾驶人意外保险(湖南)'),
('MP03250635', '平安昱乘无忧行'),
('MP03210149', '平安贵州百万车家保定制版2022款'),
('MP03200604', '平安车家保车商尊享(湖南23版)'),
('MP03000845', '平安百万车家保王牌白银版二'),
('MP02350032', '平安非营运电动单车保险(东莞互联网)'),
('MP18000198', '平安常规团意自选产品(含健康险、驾驶人、车上人员)'),
('MP01000016', '新能源汽车交通事故责任强制保险'),
('MP03200697', '平安百万车家保车商至尊版(湖南版)'),
('MP03051968', '平安百万车家保2023款(深圳GJ版)'),
('MP03100451', '平安百万车家保2023款(常州MINI)'),
('MP02260086', '平安非机动车(四川四轮)'),
('MP03410176', '平安货车驾乘意外保险2022版'),
('MP03001220', '平安全车驾乘险2022款'),
('MP03261101', '平安摩托车驾驶人意外险(川分专属版)'),
('MP18200314', '平安中车研究所团意健'),
('MP02250319', '平安非营业非机动车保险二年期'),
('MP03250504', '平安驾乘险(中交版)'),
('MP03000844', '平安百万车家保王牌标准版Plus二'),
('MP02250214', '平安非机动车保险(涡阳三年期版)'),
('MP03200538', '平安百万车家保定制版2022款(湖南)'),
('MP03001245', '平安全车驾乘险(小鹏专属5座)'),
('MP02010144', '平安小牛电动车专属'),
('MP03200607', '平安驾驶人意外保险23款(经济版)'),
('MP10410290', '综拓版-佛山-车主无忧(随车-2座)'),
('MP03140386', '平安百万车家保2023升级版(广西)'),
('MP03330096', '平安货车驾驶人员意外保险'),
('MP03251008', '平安驾乘险(互联网版)高端款'),
('MP03510232', '平安全车驾乘险2022款(优享体验)'),
('MP03250714', '平安非机动车京东专属年度保障(50万)'),
('MP03250398', '平安货车驾乘意外保险(专属)'),
('MP02260085', '平安非机动车(四川三轮)'),
('MP03250661', '平安百万车家保创展2022款'),
('MP02260036', '平安非机动车保险(营运车辆版)'),
('MP03011377', '平安驾乘无忧四代保险(北京升级版)'),
('MP02050462', '平安(深圳)电动自行车保险(存量车)'),
('MP03040715', '平安保游天下自驾游保障计划2代(互联网)'),
('MP18022398', '平安全车人员意外险(车主信用卡110万)'),
('MP03000826', '平安百万车家保精英基础版一'),
('MP03000929', '平安驾乘无忧三代保险'),
('MP02040443', '平安电动自行车三者保险(韶关)'),
('MP03210150', '平安贵州百万车家保专属版2022款'),
('MP03250750', '平安非机动车京东专属年度保障(100万)'),
('MP03001589', '平安逸家·0免赔百万医(车主尊享版)'),
('MP18200312', '平安24中车研究所'),
('MP03022132', '平安摩托车意外保险'),
('MP03250819', '平安货车驾乘险(企业互联网版)'),
('MP01000015', '新能源汽车商业保险'),
('MP03100229', '平安驾乘无忧四代保险(江苏)'),
('MP03200603', '平安百万车家保2023款(湖南)'),
('MP18021600', '平安旅云保境内自驾畅游保险'),
('MP18000195', '平安24标准页面类型自定义产品(含驾乘)'),
('MP02260084', '平安非机动车(四川二轮)'),
('MP18022241', '平安理想驾乘人员意外险'),
('MP03011307', '平安出行险(车商专属版B)'),
('MP02150032', '平安非机动车保障计划(玉禾田)'),
('MP03250527', '平安车家保全家桶V2'),
('MP03250534', '平安车家保全家桶V3'),
('MP03200502', '平安百万车家保专属版2022款(湖南版)'),
('MP03001552', '平安驾乘险2024(品牌专属)')




DROP TABLE IF EXISTS `CLAIM_DIM`.`dim_channel_gs_bisic`;


CREATE TABLE `CLAIM_DIM`.`dim_channel_gs_bisic` (
  `CHANNEL_KEY` VARCHAR(50) COMMENT '渠道key',
  `CHANNEL_VALUE` VARCHAR(50) COMMENT '渠道value',
  `PRE_EXAMINE_GS` DECIMAL(10, 2) COMMENT '预审在岗工时',
  `PRE_EXAMINE_BASIC` DECIMAL(10, 2) COMMENT '预审基数',
  `HOSPITAL_GS` DECIMAL(10, 2) COMMENT '医院在岗工时',
  `HOSPITAL_BASIC` DECIMAL(10, 2) COMMENT '医院基数',
  `DIAGNOSE_GS` DECIMAL(10, 2) COMMENT '诊断在岗工时',
  `DIAGNOSE_BASIC` DECIMAL(10, 2) COMMENT '诊断基数',
  `DETAIL_GS` DECIMAL(10, 2) COMMENT '明细在岗工时',
  `DETAIL_BASIC` DECIMAL(10, 2) COMMENT '明细基数',
  `CHARGE_GS` DECIMAL(10, 2) COMMENT '扣费在岗工时',
  `CHARGE_BASIC` DECIMAL(10, 2) COMMENT '扣费基数',
  `CONFIG_GS` DECIMAL(10, 2) COMMENT '配置在岗工时',
  `CONFIG_BASIC` DECIMAL(10, 2) COMMENT '配置基数',
  `EXAMINE_GS` DECIMAL(10, 2) COMMENT '审核在岗工时',
  `EXAMINE_BASIC` DECIMAL(10, 2) COMMENT '审核基数',
  `COMPLEX_GS` DECIMAL(10, 2) COMMENT '复核在岗工时',
  `COMPLEX_BASIC` DECIMAL(10, 2) COMMENT '复核基数',
  `MANAGE_GS` DECIMAL(10, 2) COMMENT '管理在岗工时',
  `MANAGE_BASIC` DECIMAL(10, 2) COMMENT '管理基数',
  `ZDGZ_BASIC` DECIMAL(10, 2) COMMENT '诊断规则基数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='渠道工时基数表';




CREATE TABLE `CLAIM_DIM`.`dim_cp08_policy_no` (
  channel_group_policy_no varchar(50) DEFAULT NULL COMMENT '客户团单号'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;











insert into  claim_ods.DIM_CHANNEL_DATE
select 'TK09',
'泰康养老江苏分公司',
      dim.`DATE_DT` ,
  dim.`DATE_DTD`,
  dim.`DT_MONTH`,
  dim.`DT_YEAR`,
  substr(dim.`DT_MONTH`,6,2),
  dim.`DT_DAY`,
  dim.`DT_WEEK`,
  dim.`DT_JD`,
  dim.`BOURSE_WEEK`,
  dim.`WEEK_QJ`
from CLAIM_ODS.`DIM_DATE_WEEK` dim  where  DATE_DT>='2024-10-31'



drop table if exists `CLAIM_DIM`.`DIM_CHANNEL_HOUR`;

CREATE TABLE `CLAIM_DIM`.`DIM_CHANNEL_HOUR` (
    `CHANNEL_KEY` VARCHAR(50) COMMENT '渠道code',
    `CHANNEL_VALUE` VARCHAR(50) COMMENT '渠道value',
    `HOUR` int COMMENT '年月日'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='渠道小时维表';




insert into  claim_ods.DIM_CHANNEL_DATE
select 'TK10',
'泰康养老辽宁分公司',
      dim.`DATE_DT` ,
  dim.`DATE_DTD`,
  dim.`DT_MONTH`,
  dim.`DT_YEAR`,
  substr(dim.`DT_MONTH`,6,2),
  dim.`DT_DAY`,
  dim.`DT_WEEK`,
  dim.`DT_JD`,
  dim.`BOURSE_WEEK`,
  dim.`WEEK_QJ`
from CLAIM_ODS.`DIM_DATE_WEEK` dim  where  DATE_DT>='2024-11-27'





insert into  claim_ods.DIM_CHANNEL_DATE
select 'YH01',
'湖南医慧保',
      dim.`DATE_DT` ,
  dim.`DATE_DTD`,
  dim.`DT_MONTH`,
  dim.`DT_YEAR`,
  substr(dim.`DT_MONTH`,6,2),
  dim.`DT_DAY`,
  dim.`DT_WEEK`,
  dim.`DT_JD`,
  dim.`BOURSE_WEEK`,
  dim.`WEEK_QJ`
from CLAIM_ODS.`DIM_DATE_WEEK` dim  where  DATE_DT>='2024-12-31'



insert into  claim_ods.DIM_CHANNEL_DATE
select '上海分公司',
        '太保产险上海分公司',
      dim.`DATE_DT` ,
      dim.`DATE_DTD`,
      dim.`DT_MONTH`,
      dim.`DT_YEAR`,
      substr(dim.`DT_MONTH`,6,2),
      dim.`DT_DAY`,
      dim.`DT_WEEK`,
      dim.`DT_JD`,
      dim.`BOURSE_WEEK`,
      dim.`WEEK_QJ`
from CLAIM_ODS.`DIM_DATE_WEEK` dim  where  DATE_DT>='2024-12-28';




insert into  claim_ods.DIM_CHANNEL_DATE
select 'TK11',
'泰康养老甘肃分公司',
      dim.`DATE_DT` ,
  dim.`DATE_DTD`,
  dim.`DT_MONTH`,
  dim.`DT_YEAR`,
  substr(dim.`DT_MONTH`,6,2),
  dim.`DT_DAY`,
  dim.`DT_WEEK`,
  dim.`DT_JD`,
  dim.`BOURSE_WEEK`,
  dim.`WEEK_QJ`
from CLAIM_ODS.`DIM_DATE_WEEK` dim  where  DATE_DT>='2025-05-14'


UPDATE claim_ods.DIM_CHANNEL_DATE AS cd
-- 关联DIM_DATE_WEEK表
JOIN CLAIM_ODS.DIM_DATE_WEEK AS dw ON cd.DATE_DTD = dw.DATE_DTD
SET cd.WEEK_QJ = dw.WEEK_QJ
WHERE cd.DT_YEAR = '2026';


insert into  claim_ods.DIM_CHANNEL_DATE
select 'CP11',
'太保高端医疗',
      dim.`DATE_DT` ,
  dim.`DATE_DTD`,
  dim.`DT_MONTH`,
  dim.`DT_YEAR`,
  substr(dim.`DT_MONTH`,6,2),
  dim.`DT_DAY`,
  dim.`DT_WEEK`,
  dim.`DT_JD`,
  dim.`BOURSE_WEEK`,
  dim.`WEEK_QJ`
from CLAIM_ODS.`DIM_DATE_WEEK` dim  where  DATE_DT>='2025-03-09'

insert into  claim_ods.DIM_CHANNEL_DATE
select 'CS01',
'长生人寿',
      dim.`DATE_DT` ,
  dim.`DATE_DTD`,
  dim.`DT_MONTH`,
  dim.`DT_YEAR`,
  substr(dim.`DT_MONTH`,6,2),
  dim.`DT_DAY`,
  dim.`DT_WEEK`,
  dim.`DT_JD`,
  dim.`BOURSE_WEEK`,
  dim.`WEEK_QJ`
from CLAIM_ODS.`DIM_DATE_WEEK` dim  where  DATE_DT>='2025-03-13'






INSERT INTO `CLAIM_DIM`.`dim_cp08_policy_no` (channel_group_policy_no)
VALUES
('019G24100011193'),
('019G24100009984'),
('019G24100009983'),
('AGUZ080G9925QAAAAAUB'),
('AGUZ080G9925QAAAA33A'),
('AGUZ080G9924QAAAAAWU'),
('AGUZ0802WI25QAAAAAMR'),
('AGUZ0802WI24QAAAAA7Y');





drop TABLE if exists charge_ratio_match_result;


CREATE TABLE claim_ods.charge_ratio_match_result (
    item_name_ext VARCHAR(255) COMMENT '录入名称',
    item_code VARCHAR(255) COMMENT '标准名称代码',
    item_name VARCHAR(255) COMMENT '标准名称',
    ratio VARCHAR(255) COMMENT '自负比例',
    medicare_level VARCHAR(255) COMMENT '医保等级',
    effective_date VARCHAR(255) COMMENT '有效日期',
    province_id VARCHAR(255) COMMENT '省份ID',
    province_name VARCHAR(255) COMMENT '省份名称',
    region_id VARCHAR(255) COMMENT '区域ID',
    region_name VARCHAR(255) COMMENT '区域名称',
    charge_item_type_desc VARCHAR(255) COMMENT '费用项类型'
);




UPDATE claim_dim.DIM_MANPOWER_CONFIG t1
JOIN claim_dim.DIM_MANPOWER_CONFIG t2
ON t1.CHANNEL_VALUE = t2.CHANNEL_VALUE
SET t1.CONFIG_MONEY = t2.CONFIG_MONEY,
    t1.CUSTOMER_MONEY = t2.CUSTOMER_MONEY,
    t1.CLAIM_OPERATE_MONEY = t2.CLAIM_OPERATE_MONEY,
    t1.CLAIM_STANDARD_MONEY = t2.CLAIM_STANDARD_MONEY,
    t1.CLAIM_AUDIT_MONEY = t2.CLAIM_AUDIT_MONEY,
    t1.TECH_MONEY = t2.TECH_MONEY
WHERE t1.CHANNEL_VALUE IN ('大家养老', '太保产险宁波分公司', '中国人寿财产保险', '太保产险上海分公司', '泰康养老河南分公司', '泰康养老上海分公司', '泰康养老辽宁分公司', '长生人寿', '泰康养老广东分公司', '泰康养老北京分公司', '泰康养老山东分公司', '渤海人寿', '太保产险苏州分公司', '太保健康', '泰康养老甘肃分公司', '太保产险大连分公司', '平安产险', '太保高端医疗', '泰康养老江苏分公司', '泰康养老福建分公司', '中智')
  AND t1.DT_MONTH IN ('2025-01', '2025-02', '2025-03', '2025-04')
  AND t2.DT_MONTH = '2025-05';