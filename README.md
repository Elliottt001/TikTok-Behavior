# TikTok-Behavior

这是一个模拟 TikTok 用户行为分析的数据仓库项目。该项目包含完整的数据生成、ETL（抽取、转换、加载）处理流程以及报表生成。项目使用 Python 进行流程编排，DuckDB 作为核心计算引擎。

## 项目简介

本项目旨在模拟一个短视频平台的数据处理流程。主要功能包括：
1.  **数据生成**：使用 `Faker` 库并行生成模拟的用户数据、视频数据和用户行为日志。
2.  **ETL 流程**：
    *   **ODS 层 (Operational Data Store)**：将生成的 CSV 和 JSONL 原始数据加载到 DuckDB 数据库中。
    *   **DWD 层 (Data Warehouse Detail)**：对原始数据进行清洗、去重和规范化处理。
    *   **数据存储**：将清洗后的 DWD 层数据导出为 Parquet 格式，以便于高效存储和分析。
    *   **ADS 层 (Application Data Service)**：基于 DWD 层数据生成业务报表。

## 项目结构

```
TikTok-Behavior/
├── main.py                 # 项目主入口，编排整个 pipeline
├── data/                   # 数据存储目录
│   ├── raw/                # 生成的原始数据 (CSV, JSONL)
│   └── ...                 # 生成的 Parquet 文件和 DuckDB 数据库文件
├── scripts/                # Python 脚本目录
│   ├── data_gen.py         # 数据生成脚本 (多进程)
│   ├── etl_job.py          # SQL 执行器
│   ├── db_connector.py     # DuckDB 连接工具
│   └── parquet_torage.py   # Parquet 导出工具
└── sql/                    # SQL 脚本目录
    ├── ods_load.sql        # ODS 层加载 SQL
    ├── dwd_cleansing.sql   # DWD 层清洗 SQL
    └── ads_report.sql      # ADS 层报表 SQL
```

## 环境依赖

*   Python 3.12
*   DuckDB
*   Faker
*   Pandas

## 快速开始

1.  **克隆项目**
    ```bash
    git clone https://github.com/Elliottt001/TikTok-Behavior.git
    cd TikTok-Behavior
    ```

2.  **运行 Pipeline**
    直接运行 `main.py` 即可执行完整流程：
    ```bash
    python main.py
    ```

    该脚本将按顺序执行以下步骤：
    1.  并行生成模拟数据（默认生成 500万条 CSV 记录和 3000万条 JSONL 记录）。
    2.  执行 `sql/ods_load.sql` 加载数据。
    3.  执行 `sql/dwd_cleansing.sql` 清洗数据。
    4.  将 DWD 表导出为 Parquet 文件至 `data/` 目录。
    5.  执行 `sql/ads_report.sql` 生成报表。

## 数据模型

*   **Users**: 用户基础信息 (CSV)
*   **Videos**: 视频基础信息 (CSV)
*   **Behavior Logs**: 用户行为日志 (JSONL)，包含点赞、评论、转发等行为。

## 技术栈

*   **编程语言**: Python
*   **数据库/计算引擎**: DuckDB
*   **数据格式**: CSV, JSONL, Parquet
