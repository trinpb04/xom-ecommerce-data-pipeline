# Xom Ecommerce Data Pipeline - Full Roadmap

**Đích đến:** SQL Server (schema `e_commerce`) -> dlt (Python) -> Snowflake -> dbt -> Airflow (Docker/Astro) -> Power BI
**Môi trường:** Ubuntu / WSL2
**Repo:** `xom-ecommerce-data-pipeline`

## Bức tranh tổng thể

Pipeline như một nhà máy nước:

- **SQL Server** = cái giếng. Data gốc dạng OLTP.
- **dlt** = máy bơm. Script Python hút data từ giếng, bơm lên bồ. EL = Extract + Load.
- **Snowflake** = bồ chứa nước thô. Data thô nằm ở `ecommerce_db.e_commerce`, data sạch nằm ở `ecommerce_db.dev`.
- **dbt** = nhà máy lọc. Biến data thô thành data sạch để phân tích (T = Transform).
- **Airflow** = công tắc hẹn giờ. Tự chạy máy bơm rồi nhà máy lọc mỗi ngày.
- **Power BI** = vòi nước cuối. Mở dashboard là thấy data sạch.

## Bảng tiến độ

| Phase | Việc | Trạng thái |
|---|---|---|
| 0 | Repo + khung thư mục | XONG |
| 1 | Khảo sát schema `e_commerce` | XONG (còn nợ `docs/data model.md`) |
| 2 | dlt EL: SQL Server -> Snowflake | XONG |
| 3 | dbt: staging -> intermediate -> marts + tests | XONG (còn nợ `month` -> `month_num` trong dim_date) |
| 4 | Airflow + Docker | XONG - DAG chạy xanh toàn bộ 2026-07-19 |
| 5 | Power BI | CHƯA LÀM |
| 6 | README | CHƯA LÀM |

---

# PHASE 0: Repo và khung thư mục

```
xom-ecommerce-data-pipeline/
├── .env                        # credentials SQL Server - KHÔNG commit
├── .gitignore
├── requirements-el.txt         # freeze venv dlt
├── el/                         # EL layer (dlt)
│   ├── .dlt/
│   │   ├── config.toml
│   │   └── secrets.toml        # credentials - KHÔNG commit
│   ├── explore_source.py
│   ├── pipeline_ecommerce_v1_fullload.py
│   └── pipeline_ecommerce_v2_incremental.py
├── dbt_ecommerce/              # T layer (dbt)
│   ├── dbt_project.yml
│   ├── packages.yml            # dbt-labs/dbt_utils
│   ├── requirements-dbt.txt    # freeze venv dbt
│   ├── macros/
│   │   └── date_spine_for_ecom_sales.sql
│   ├── models/
│   │   ├── staging/            # 4 model + 4 yml (source-level tests)
│   │   ├── intermediate/       # 2 model + 2 yml
│   │   └── marts/
│   │       ├── dimension/      # 4 model + 4 yml
│   │       └── fact/           # 2 model + 2 yml
│   └── tests/                  # 5 singular tests
├── airflow/                    # Orchestration (Astro project)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── .env                    # credentials dlt cho container - KHÔNG commit
│   ├── airflow_settings.yaml   # connection Snowflake local dev - KHÔNG commit
│   ├── dags/
│   │   └── ecommerce_elt_dag.py
│   └── include/                # bản copy dbt_ecommerce/ + el/ cho container
└── docs/
    ├── schema_dump.txt
    └── data model.md           # CHƯA CÓ - còn nợ
```

Quy tắc bảo mật: `.env`, `secrets.toml`, `airflow/.env`, `airflow_settings.yaml` đều nằm trong `.gitignore`. Verify định kỳ bằng `git ls-files` (không được thấy file secrets nào) và `git check-ignore <file>`.

---

# PHASE 1: Khảo sát SQL Server schema `e_commerce`

Kết quả nằm trong `docs/schema_dump.txt`. 4 bảng core:

| Bảng | Vai trò | PK | Cột |
|---|---|---|---|
| `ecom_sales` | **Fact** - 1 dòng = 1 sản phẩm trong 1 đơn | `row_id` | row_id, order_id, order_date, customer_id, segment, region_code, product_code, quantity, sales, discount, profit |
| `customer` | Dim khách hàng | `customer_id` | + first/last_name, birth_date, marital_status, gender, email_address, annual_income, education_level, occupation, home_owner |
| `product` | Dim sản phẩm | `product_code` | product, category, subcategory |
| `region` | Dim địa lý | `region_code` | city, state, country, lat/long, region, market |

**Đặc điểm data đã verify (quyết định nhiều thiết kế phía sau):**

- `ecom_sales` KHÔNG có cột `updated_at` -> incremental chỉ có thể dựa vào `order_date`.
- `DISCOUNT` là % (0-0.7), `PROFIT` là tiền lời tuyệt đối, `SALES` là giá gốc CHƯA trừ discount (verify bằng cách so unit_price cùng product_code ở các mức discount khác nhau - không đổi).
- `SEGMENT` trong ecom_sales là phân khúc kinh doanh có sẵn (Consumer/Corporate/Home Office) - đổi tên thành `customer_segment` ở tầng intermediate.
- Mã `region_code`/`product_code` có dòng bị lặp prefix (kiểu `RR0001`) - phải làm sạch ở staging, không thì gãy join với dim.
- Quyết định phạm vi: KHÔNG làm RFM - thay bằng `fct_customer_summary` đơn giản, không chấm điểm/gán nhãn.

Còn nợ: ghi row count + mô tả bảng vào `docs/data model.md`.

---

# PHASE 2: dlt EL - SQL Server vào Snowflake

Credentials khai trong `el/.dlt/secrets.toml`: source SQL Server qua `mssql+pyodbc` (cần ODBC Driver 18 + `TrustServerCertificate=yes`), destination Snowflake (user/pass/role/warehouse/database).

## v1 - full load (`pipeline_ecommerce_v1_fullload.py`) - bản đang dùng

```python
import dlt
from dlt.sources.sql_database import sql_database

CORE_TABLES = ["customer", "ecom_sales", "product", "region"]

def run_full_load():
    # schema="e_commerce": CHỈ quét schema này, bỏ qua các schema khác
    source = sql_database(schema="e_commerce").with_resources(*CORE_TABLES)

    pipeline = dlt.pipeline(
        pipeline_name="ecommerce_mssql_to_snowflake",
        destination="snowflake",
        dataset_name="e_commerce",   # = tên schema đích bên Snowflake
        progress="log",
    )

    # replace = xóa bảng cũ, ghi lại từ đầu -> idempotent, chạy lại thoải mái
    info = pipeline.run(source, write_disposition="replace")
    print(info)
```

`dataset_name="e_commerce"` -> data thô nằm ở **`ecommerce_db.e_commerce`**. Source trong dbt trỏ theo đúng schema này (source name `e_commerce` không khai `schema:` thì dbt mặc định schema = tên source, nên khớp).

## v2 - incremental (`pipeline_ecommerce_v2_incremental.py`) - để dành

Chỉ dùng khi data nguồn LỚN và có dòng mới thường xuyên. Hai điểm sống còn:

1. Mốc incremental là `order_date` với `initial_value=date(2018, 1, 1)` (kiểu `date` vì cột nguồn là DATE) - vì nguồn không có `updated_at`.
2. `primary_key` PHẢI là **`row_id`** - grain là 1 dòng/sản phẩm. Merge theo `order_id` sẽ đè 1 đơn nhiều sản phẩm còn đúng 1 dòng -> mất data.

Lưu ý khi cân nhắc chuyển sang v2: nó chỉ cover `ecom_sales` - 3 bảng dim vẫn phải full load kèm theo, không thì dim stale.

### Checkpoint Phase 2
- [x] Row count Snowflake khớp SQL Server
- [x] Data nằm trong `ecommerce_db.e_commerce`

---

# PHASE 3: dbt - staging, intermediate, marts

## Kiến trúc lớp

```
staging (view)                          # đổi tên cột + làm sạch mã, KHÔNG join
├── e_commerce_ecom_sales               # region_id/product_id REGEXP làm sạch
├── e_commerce_customer                 # customer_id -> id, email_address -> email...
├── e_commerce_product                  # product_code -> id, product -> product_name
└── e_commerce_region                   # region_code -> id

intermediate (table)
├── int_sales_enriched                  # join dim + 5 chỉ số phái sinh
└── int_customer_order_summary          # tổng hợp theo customer

marts/dimension (table)
├── dim_customer                        # + full_name, age
├── dim_product
├── dim_region
└── dim_date                            # spine tự động theo min/max order_date

marts/fact (table)
├── fct_sales                           # grain: 1 dòng = 1 sản phẩm trong 1 đơn
└── fct_customer_summary                # grain: 1 dòng = 1 khách hàng
```

Materialization trong `dbt_project.yml`: staging = view, intermediate + marts = table, tất cả chạy trên warehouse `ecommerce_wh`. Models build vào schema **`dev`** (theo profile), source đọc từ schema **`e_commerce`**.

## Quy ước đặt tên

- Khóa chính của MỌI bảng dim đặt là `id`. Khóa ngoại trong fact: `customer_id`, `product_id`, `region_id`.
- Rename: `email_address -> email`, `education_level -> edu_level`, `product -> product_name`, `subcategory -> sub_category`, `segment -> customer_segment`.
- Trade-off đã biết: vào Power BI, 3 bảng dim đều có cột tên `id` - nhìn Model view phải dựa vào tên bảng để phân biệt. Chấp nhận được; nếu muốn đổi thì đổi TRƯỚC khi làm Phase 5 (đổi sau sẽ gãy relationship + visual).

## Staging

1. **Làm sạch mã bẩn ngay tại staging** (`e_commerce_ecom_sales.sql`):
   ```sql
   REGEXP_REPLACE(REGION_CODE, '^R+', 'R') as region_id,
   REGEXP_REPLACE(PRODUCT_CODE, '^P+', 'P') as product_id,
   ```
   Rủi ro cần nhớ: nếu nguồn tồn tại đồng thời `R001` và `RR001` thì sau khi clean sẽ trùng nhau - test `relationships` ở fct_sales là lưới an toàn.
2. **Test đặt ở tầng SOURCE** (trong 4 file yml staging): `unique`/`not_null` cho PK bảng raw. Cột sau khi rename/clean được test gián tiếp qua test `unique`/`not_null` trên `id` của các dim.

## Intermediate

### `int_sales_enriched.sql` - grain: 1 dòng = 1 sản phẩm trong đơn

Join sales với product + region (LEFT JOIN để không âm thầm mất dòng khi mã không khớp) và tính **5 chỉ số phái sinh**:

| Chỉ số | Công thức | Ý nghĩa |
|---|---|---|
| `net_sales` | `sales * (1 - coalesce(discount, 0))` | Doanh thu THỰC nhận sau chiết khấu |
| `profit_margin` | `profit / sales` (NULL khi sales=0) | Tỷ suất lợi nhuận |
| `unit_price` | `round(sales / quantity, 2)` (NULL khi quantity=0) | Giá mỗi đơn vị TRƯỚC discount |
| `discount_amount` | `round(sales * coalesce(discount, 0), 2)` | Số tiền $ thực tế đã giảm |
| `is_profitable` | `profit > 0` | Cờ lọc nhanh giao dịch lời/lỗ |

Lưu ý test: KHÔNG đặt `not_null` cho `profit_margin` (công thức cố tình trả NULL khi sales=0) và cho `discount` (raw pass-through, `net_sales` đã coalesce phòng NULL).

### `int_customer_order_summary.sql` - grain: 1 dòng = 1 khách hàng

`min/max(order_date)`, `count(distinct order_id)` (KHÔNG `count(*)` - sẽ thổi phồng số đơn theo số sản phẩm), `sum(net_sales)`, `sum(profit)`.

## Dimension marts

- **`dim_customer`**: + `full_name`, + `age` = `datediff(year, birth_date, current_date())` (tuổi là thuộc tính thật của người -> dùng current_date() là đúng).
- **`dim_product`**, **`dim_region`**: pass-through từ staging.
- **`dim_date`**: macro tự viết `macros/date_spine_for_ecom_sales.sql` bọc `dbt_utils.date_spine`, start/end lấy động từ min/max(order_date) của staging - spine tự co giãn theo data, không hardcode năm. Output: date_day, year, quarter, quarter_name, month, month_name.

## Fact marts

- **`fct_sales`** - grain 1 dòng = 1 sản phẩm trong 1 đơn. Pass-through từ int_sales_enriched: khóa (row_id, order_id, customer_id, product_id, region_id), customer_segment, số liệu gốc (quantity, sales, discount, profit) + 5 chỉ số phái sinh. Test: `row_id` unique + not_null; `relationships` từ 3 FK về `id` của 3 dim (syntax `arguments:` của dbt 1.10+) - phát hiện orphan record làm Power BI relationship gãy âm thầm.
- **`fct_customer_summary`** - grain 1 dòng = 1 khách hàng. `days_since_last_order` so với **max(last_order_date) trong data** (không dùng current_date() vì data bán hàng là data lịch sử), `is_returning_customer` = total_orders > 1. Test: customer_id unique + not_null.

## Singular tests (5 file trong `tests/`, PASS khi trả về 0 dòng)

| File | Rule |
|---|---|
| `assert_no_negative_quantity.sql` | quantity phải > 0 |
| `assert_discount_within_valid_range.sql` | 0 <= discount <= 1 |
| `assert_net_sales_not_greater_than_sales.sql` | net_sales <= sales |
| `assert_profit_margin_reasonable.sql` | -1 <= profit_margin <= 1 (fail = outlier đáng xem, không hẳn code sai) |
| `assert_order_date_not_future.sql` | `order_date > current_date()` là lỗi nhập liệu/ETL |

Test fail KHÔNG tự động nghĩa là code sai - xem dòng vi phạm rồi mới quyết sửa code hay chấp nhận data thật.

## Lệnh chạy

```bash
dbt deps          # cài dbt_utils
dbt run
dbt test
dbt docs generate && dbt docs serve   # xem lineage graph
```

### Checkpoint Phase 3
- [x] `dbt run` + `dbt test` pass toàn bộ (đã verify qua DAG chạy xanh)
- [x] count(*) int_sales_enriched khớp staging
- [ ] Đổi cột `month` -> `month_num` trong `dim_date.sql` (Power BI cần làm Sort by Column cho `month_name` - làm trước Phase 5)
- [ ] Viết `docs/data model.md`

---

# PHASE 4: Airflow + Docker - XONG (DAG chạy xanh toàn bộ)

## Setup

```bash
curl -sSL install.astronomer.io | sudo bash -s
cd ~/projects/xom-ecommerce-data-pipeline/airflow
astro dev init
```

Astro Runtime 3.x = **Airflow 3** -> DAG import từ `airflow.sdk`, không phải `airflow.decorators`.

## 4a: `airflow/Dockerfile`

Hai việc: (1) cài **ODBC Driver 18** - dlt đọc SQL Server qua `mssql+pyodbc`, thiếu driver là task extract crash `Can't open lib 'ODBC Driver 18 for SQL Server'`; (2) venv riêng cho dbt để không conflict dependency với Airflow.

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.3-2

# ODBC Driver 18 cho pyodbc (dlt doc SQL Server qua mssql+pyodbc)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl gnupg2 \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc \
    && rm -rf /var/lib/apt/lists/*
USER astro

# venv rieng cho dbt - goi thang dbt_venv/bin/pip, khong can source/activate
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir dbt-snowflake
```

## 4b: `airflow/requirements.txt`

```
astronomer-cosmos
apache-airflow-providers-snowflake
dlt[sql_database,snowflake]
pyodbc
```

Extra `sql_database` (SQLAlchemy source của dlt) và `pyodbc` là bắt buộc - `dlt[snowflake]` không kéo theo.

## 4c: Copy code vào `include/`

Container chỉ thấy file bên trong `airflow/`. Chạy từ root repo:

```bash
rsync -a --delete --exclude target --exclude logs --exclude .claude dbt_ecommerce/ airflow/include/dbt_ecommerce/
rsync -a --delete --exclude .dlt --exclude __pycache__ el/ airflow/include/el/
```

- GIỮ `dbt_packages/` khi copy (chứa dbt_utils); trong DAG vẫn để `install_deps: True` làm lưới an toàn.
- KHÔNG copy `.dlt/secrets.toml` - credentials đưa qua env vars (4d).
- `include/` được mount live -> sửa file trong đó không cần rebuild; nhưng sửa `dbt_ecommerce/` gốc thì phải rsync lại. Nợ kỹ thuật chấp nhận: tồn tại 2 bản copy dbt project.

## 4d: Credentials cho dlt - `airflow/.env`

Trong container dlt KHÔNG đọc được `el/.dlt/secrets.toml` (nó tìm `.dlt/` theo thư mục làm việc của process). Astro tự nạp `airflow/.env` thành env vars trong container - dlt nhận credentials qua 2 connection string:

```
SOURCES__SQL_DATABASE__CREDENTIALS=mssql+pyodbc://<user>:<password>@<host>:1433/<database>?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
DESTINATION__SNOWFLAKE__CREDENTIALS=snowflake://<user>:<password>@<account>/ECOMMERCE_DB?warehouse=ECOMMERCE_WH&role=ecommerce_role
```

- Dùng 1 connection string thay vì tách từng key: phần `query` (driver, TrustServerCertificate) là dict - dlt không resolve được key con của dict qua env vars tách rời.
- Ký tự đặc biệt trong password phải URL-encode (`%` -> `%25`, `@` -> `%40`).
- `<account>` Snowflake = đúng giá trị `host` trong secrets.toml.
- File này chứa password -> đã nằm trong `airflow/.gitignore`, TUYỆT ĐỐI không commit.

## 4e: Connection Snowflake cho dbt - `airflow/airflow_settings.yaml`

Astro tự nạp khi `astro dev start` (local dev only, đã gitignored):

```yaml
airflow:
  connections:
    - conn_id: snowflake_conn
      conn_type: snowflake
      conn_schema: dev
      conn_login: <user>
      conn_password: <password>
      conn_extra:
        account: <account>
        warehouse: ecommerce_wh
        database: ecommerce_db
        role: ecommerce_role
```

## 4f: DAG - `airflow/dags/ecommerce_elt_dag.py`

```python
from pendulum import datetime
from airflow.sdk import dag, task  # Airflow 3 (Astro Runtime 3.x)
from cosmos import (
    DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig,
)
from cosmos.constants import InvocationMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt_ecommerce"
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_ecommerce",  # phai trung `profile:` trong dbt_project.yml
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={"database": "ecommerce_db", "schema": "dev"},
    ),
)


@dag(
    dag_id="ecommerce_elt_dag",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,      # nguon khong partition theo ngay -> backfill vo nghia
    max_active_runs=1,  # 2 run chong nhau se dua nhau replace bang raw
    default_args={"owner": "trinpb04", "retries": 2},
    tags=["ecommerce", "elt"],
)
def ecommerce_elt_dag():

    @task
    def extract_load():
        # v1 full load ca 4 bang (replace, idempotent) - v2 incremental chi cover
        # ecom_sales, chay minh no thi 3 bang dim khong bao gio duoc refresh.
        # Import TRONG task: thieu lib/driver chi lam fail task nay, khong lam
        # ca DAG import error.
        from include.el.pipeline_ecommerce_v1_fullload import run_full_load
        run_full_load()

    transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            # dbt nam trong venv rieng -> phai chay bang subprocess;
            # mac dinh DBT_RUNNER doi hoi dbt cai chung voi Airflow
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        # render (dbt ls luc parse DAG) cung phai tro vao dbt trong venv,
        # vi PATH cua Airflow khong co dbt. RenderConfig co invocation_mode
        # RIENG (mac dinh DBT_RUNNER) -> cung phai ep SUBPROCESS
        render_config=RenderConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        operator_args={"install_deps": True},  # dam bao dbt_utils co mat
    )

    extract_load() >> transform


ecommerce_elt_dag()
```

Vì sao từng lựa chọn:

| Lựa chọn | Lý do |
|---|---|
| `airflow.sdk` thay vì `airflow.decorators` | Runtime 3.x = Airflow 3, SDK mới là chuẩn |
| Gọi v1 full load chứ không phải v2 incremental | v2 chỉ cover ecom_sales, dims sẽ stale; data nhỏ nên full load daily rẻ và idempotent |
| Import dlt bên trong task | Lỗi thiếu pyodbc/driver chỉ fail task extract, DAG vẫn parse được để debug |
| `catchup=False` + `max_active_runs=1` | Nguồn không partition theo ngày; 2 run song song sẽ tranh nhau `replace` bảng raw |
| `InvocationMode.SUBPROCESS` ở CẢ ExecutionConfig lẫn RenderConfig | dbt nằm trong venv riêng; mặc định DBT_RUNNER đòi dbt cài chung env với Airflow, sẽ raise `CosmosValueError` ngay lúc parse |
| Cosmos `DbtTaskGroup` thay vì 1 BashOperator `dbt run && dbt test` | Mỗi model + test thành task riêng: thấy được model nào fail, retry đúng chỗ, lineage hiện trên UI |
| `retries: 2` | Mạng chập chờn thì tự chạy lại, khỏi trigger tay |

Cosmos mặc định chạy test NGAY SAU mỗi model (`TestBehavior.AFTER_EACH`) - `dbt test` đã nằm trong TaskGroup, không cần task test riêng.

## Lịch chạy tự động

Chỉnh ở tham số `schedule` trong `@dag(...)`. Cron 5 trường `phút giờ ngày tháng thứ`, mặc định tính theo **UTC** (VN = UTC+7):

```python
schedule="0 2 * * *"     # 02:00 UTC = 9h sang VN moi ngay (dang dung)
schedule="@daily"        # 00:00 UTC = 7h sang VN
schedule=None            # tat lich, chi chay khi trigger tay
```

Muốn viết cron theo giờ VN luôn: `start_date=datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh")` rồi cron được hiểu theo giờ VN. Sửa xong không cần restart - `dags/` mount live, Airflow tự re-parse sau ~30 giây.

## Chạy + debug

```bash
cd airflow
astro dev start     # UI o localhost:8080 (admin/admin)
# doi Dockerfile/requirements.txt -> `astro dev restart` (rebuild image)
# sua dags/ va include/ -> mount live, khong can restart
```

Debug import error: `astro dev run dags list-import-errors` (hoặc `astro dev bash` rồi chạy lệnh airflow trực tiếp). Lỗi ở bước Cosmos render = 99% dbt parse fail (chạy `dbt parse` local xem) hoặc thiếu InvocationMode/executable path như bảng trên.

Lỗi hạ tầng từng gặp trên WSL2: `dial tcp: lookup ... i/o timeout` khi docker pull = WSL mất DNS tạm thời, không phải lỗi code. Thử lại là hết; nếu tái phát nhiều: tắt auto-generate resolv.conf (`/etc/wsl.conf` thêm `[network] generateResolvConf = false`, ghi `nameserver 8.8.8.8` vào `/etc/resolv.conf`, rồi `wsl --shutdown`).

### Checkpoint Phase 4
- [x] Dockerfile build pass
- [x] `astro dev start` không lỗi, DAG không import error
- [x] Trigger tay: `extract_load` xanh, toàn bộ `dbt_transform` xanh (run + test từng model)
- [x] Lịch chạy tự động hoạt động
- [ ] Xóa `dags/exampledag.py` + test mẫu trước khi commit

---

# PHASE 5: Power BI - CHƯA LÀM

## Nguyên tắc

Toàn bộ logic nằm trong dbt, Power BI chỉ hiển thị. Không viết DAX phức tạp.

**Làm trước:** đổi cột `month` -> `month_num` trong `dim_date.sql` (nợ Phase 3) để set Sort by Column cho `month_name`.

## Kết nối

Get Data -> Snowflake. Server `org-account.snowflakecomputing.com`, Warehouse `ecommerce_wh`. **Import mode**. Chỉ chọn bảng mart (`fct_`, `dim_`) trong schema `dev`, KHÔNG kéo schema thô `e_commerce`.

Model view - nối fact -> dim theo key thực tế, ra hình ngôi sao, 2 fact chia sẻ dim_customer (conformed dimension):

- `fct_sales.customer_id -> dim_customer.id`
- `fct_sales.product_id -> dim_product.id`
- `fct_sales.region_id -> dim_region.id`
- `fct_sales.order_date -> dim_date.date_day`
- `fct_customer_summary.customer_id -> dim_customer.id`

## Map data vào 3 dashboard

### Dashboard 1: Overview

| Thành phần | Nguồn |
|---|---|
| KPI: tổng net_sales, profit, số đơn, số khách | `fct_sales`, `dim_customer` |
| Trend doanh thu theo tháng | `fct_sales` + `dim_date` |
| Doanh thu theo market/region | `fct_sales` + `dim_region` |
| Doanh thu theo customer_segment | `fct_sales.customer_segment` |

### Dashboard 2: Product

| Thành phần | Nguồn |
|---|---|
| Doanh thu/lợi nhuận theo category, sub_category | `fct_sales` + `dim_product` |
| Top N sản phẩm theo net_sales | `fct_sales` + `dim_product` |
| Quan hệ discount vs profit_margin (scatter) | `fct_sales.discount`, `fct_sales.profit_margin` |
| Sản lượng (quantity) theo thời gian | `fct_sales` + `dim_date` |
| So sánh unit_price giữa sản phẩm/category | `fct_sales.unit_price` |
| Tổng discount_amount theo category | `fct_sales.discount_amount` + `dim_product` |
| % đơn hàng lỗ (is_profitable=false) theo category | `fct_sales.is_profitable` + `dim_product` |

### Dashboard 3: Customers (không RFM)

| Thành phần | Nguồn |
|---|---|
| Nhân khẩu học: tuổi, giới tính, thu nhập, học vấn | `dim_customer` (age, gender, annual_income, edu_level) |
| Top khách hàng theo total_net_sales | `fct_customer_summary` |
| Tỷ lệ khách mới vs quay lại, đóng góp doanh thu | `fct_customer_summary.is_returning_customer` |
| Phân bố days_since_last_order | `fct_customer_summary` |
| Phân bố total_orders (bao nhiêu % chỉ mua 1 lần) | `fct_customer_summary` |
| Doanh thu theo customer_segment | `fct_sales.customer_segment` |

### Checkpoint Phase 5
- [ ] Model view ra hình ngôi sao
- [ ] Refresh chạy được
- [ ] Không DAX phức tạp

---

# PHASE 6: README - CHƯA LÀM

```markdown
# Xom Ecommerce Data Pipeline

Một câu mô tả project.

## Architecture
SQL Server (e_commerce) -> dlt -> Snowflake -> dbt -> Airflow -> Power BI

## Stack
Bảng tool theo layer.

## Data model
Star schema diagram + giải thích grain của fct_sales và fct_customer_summary.

## How to run
Các bước clone về chạy lại được: venv + secrets + dbt + astro dev start.

## What I learned
Phần giá trị nhất - viết thật: vụ SALES chưa trừ discount, vì sao phải REGEXP
clean mã region/product, vì sao bỏ RFM, các gotcha của Cosmos/Airflow 3.
```

Rà soát bảo mật lần cuối:

```bash
git log -p | grep -i -E "password|secret|token" | head -20
```

Không ra gì = sạch.

### Checkpoint Phase 6
- [ ] README có architecture diagram
- [ ] Người lạ clone repo, đọc README, hiểu được project

---

# Việc tiếp theo (thứ tự)

1. Đổi `month` -> `month_num` trong `dim_date.sql`, rsync lại `include/`, chạy lại DAG verify xanh
2. Viết `docs/data model.md` (row count, PK/FK, mô tả bảng)
3. Xóa `dags/exampledag.py` + test mẫu, commit + push toàn bộ
4. Phase 5: Power BI
5. Phase 6: README

Kẹt chỗ nào quá 45 phút: dừng, copy nguyên error, quay lại hỏi kèm output lệnh bị fail.
