
# INSURE_DATA_HOUSE - 保险数据仓库

## 项目简介
保险数据仓库是一个基于阿里云生态的大数据分析平台，用于处理和分析保险业务相关数据。

## 技术架构

### 数据架构

#### 数据同步
- 使用阿里云DTS（Data Transmission Service）进行数据同步
- 支持实时数据同步和离线数据同步

#### 数据存储
- 采用阿里云ADB（AnalyticDB）作为主要存储引擎
- 提供高性能的实时数据分析能力

#### 任务调度
- 使用Azkaban作为工作流调度系统
- 支持复杂的任务依赖关系管理
- 提供可视化的任务监控界面

#### 数据可视化
- 使用FineBI作为报表展示工具
- 支持丰富的图表类型和交互功能

### 数据分层

#### ODS层（Operation Data Store）
- 原始数据层
- 保持数据原貌不做处理
- 为后续数据处理提供数据来源

#### DWD层（Data Warehouse Detail）
- 明细数据层
- 对ODS层数据进行清洗转换
- 提供业务过程的详细数据

#### DWS层（Data Warehouse Service）
- 服务数据层
- 基于DWD层构建面向主题的汇总数据
- 支持多维度数据分析

## 项目结构

```plaintext
├── adb_report/              # ADB报表相关代码
│   ├── analyse/             # 数据分析脚本
│   ├── check/               # 数据检查脚本
│   ├── control_panel/       # 控制面板相关代码
│   ├── email/               # 邮件报告脚本
│   └── month_report/        # 月度报告生成脚本
├── ddl/                     # 数据定义语言脚本
│   ├── adm_ddl.sql         # 管理表DDL
│   ├── base_ddl.sql        # 基础表DDL
│   ├── dim_ddl.sql         # 维度表DDL
│   ├── dwd_ddl.sql         # DWD层表DDL
│   ├── dws_ddl.sql         # DWS层表DDL
│   └── inphile_ods_ddl.sql # ODS层表DDL
├── config.ini              # 配置文件
├── config.py               # 配置管理模块
├── database.py             # 数据库连接管理
└── emailSender.py          # 邮件发送模块
```

## 主要功能

1. 数据集成
   - 支持多源数据接入
   - 实时数据同步
   - 数据质量控制

2. 数据处理
   - 数据清洗和转换
   - 数据标准化处理
   - 数据一致性检查

3. 数据分析
   - 多维度数据分析
   - 风险分析报告
   - 业务监控指标

4. 报表服务
   - 定期报告生成
   - 自动邮件推送
   - 可视化数据展示

## 运维管理

1. 任务监控
   - Azkaban任务调度监控
   - 数据同步状态监控
   - 系统性能监控

2. 数据质量
   - 数据完整性检查
   - 数据准确性验证
   - 异常数据告警

3. 系统维护
   - 定期数据备份
   - 系统日志管理
   - 性能优化建议
        