<h1 align="center">🛒 Xom Ecommerce Data Pipeline</h1>

<p align="center">
  <em>Kéo dữ liệu bán hàng từ SQL Server, làm sạch, và biến nó thành 3 cái dashboard mà sếp mở ra là hiểu ngay.</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/SQL_Server-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white" />
  <img src="https://img.shields.io/badge/dlt-3B82F6?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" />
  <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" />
  <img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" />
  <img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/pipeline-passing-brightgreen?style=flat-square" />
  <img src="https://img.shields.io/badge/dbt_tests-10_passed-brightgreen?style=flat-square" />
  <img src="https://img.shields.io/badge/orchestration-daily_@_9AM_VN-blue?style=flat-square" />
  <img src="https://img.shields.io/badge/dashboards-3-orange?style=flat-square" />
</p>

---

## Tóm tắt nhanh

Đây là project mình build một pipeline hoàn chỉnh: dữ liệu ecommerce nằm trong SQL Server, mình hút nó qua **dlt**, đổ vào **Snowflake**, làm sạch + mô hình hóa bằng **dbt**, cho **Airflow** chạy tự động mỗi sáng, rồi cắm **Power BI** vào để ra dashboard.

Mục tiêu không phải "cho có" mà là làm đúng như production: có test, có orchestration, có star schema tử tế, và mỗi quyết định đều có lý do (mình ghi hết ở phần [What I learned](#what-i-learned) bên dưới).

```
SQL Server ──dlt──▶ Snowflake (raw) ──dbt──▶ Snowflake (clean) ──▶ Power BI
                          └──────── Airflow chạy tự động mỗi ngày ────────┘
```

Nghĩ đơn giản như một nhà máy nước: SQL Server là cái giếng, dlt là máy bơm, Snowflake là bồn chứa, dbt là hệ thống lọc, Airflow là cái công tắc hẹn giờ, còn Power BI là vòi nước cuối — mở ra là có nước sạch xài.

---

## 🧱 Stack & lý do chọn

| Layer | Tool | Tại sao dùng nó |
|---|---|---|
| Source | **SQL Server** | Dữ liệu gốc dạng OLTP, schema `e_commerce` |
| Extract + Load | **dlt** (Python) | Viết vài dòng Python là hút xong 4 bảng, tự lo schema bên Snowflake |
| Warehouse | **Snowflake** | Tách rõ raw (`e_commerce`) và clean (`dev`) trong cùng 1 database |
| Transform | **dbt** | staging → intermediate → marts, kèm test — logic nằm hết ở đây |
| Orchestration | **Airflow + Docker + Cosmos** | Chạy tự động mỗi ngày, mỗi model dbt là 1 task riêng nhìn được trên UI |
| BI | **Power BI** (Import mode) | 3 dashboard: Overview, Product, Customers |

---

## 🗂️ Data model

Nguồn có đúng 4 bảng, mình giữ nguyên tinh thần đó khi lên star schema:

| Bảng nguồn | Là gì | Khóa chính |
|---|---|---|
| `ecom_sales` | Fact — 1 dòng = 1 sản phẩm trong 1 đơn | `row_id` |
| `customer` | Khách hàng | `customer_id` |
| `product` | Sản phẩm | `product_code` |
| `region` | Địa lý | `region_code` |

Qua dbt thì chia 3 lớp:

```
staging (view)         →  đổi tên cột + làm sạch mã, KHÔNG join gì hết
intermediate (table)   →  join dim vào, tính chỉ số phái sinh
marts (table)          →  star schema đưa thẳng vào Power BI
   ├── dim_customer / dim_product / dim_region / dim_date
   └── fct_sales / fct_customer_summary
```

Điểm mình thấy đáng nói: **2 bảng fact dùng chung `dim_customer`** (conformed dimension), nên từ trang Product nhảy sang Customers vẫn nhất quán số liệu.

- `fct_sales` — grain 1 dòng/sản phẩm/đơn. 5 chỉ số (`net_sales`, `profit_margin`, `unit_price`, `discount_amount`, `is_profitable`) mình tính sẵn ở dbt luôn, nên Power BI gần như **không phải viết DAX** gì phức tạp.
- `fct_customer_summary` — grain 1 dòng/khách. Tổng hợp hành vi mua trên toàn bộ lịch sử. Cố tình **không làm RFM** — thấy chưa cần thiết cho scope này.

Cả pipeline có **10 test dbt**: mấy cái `unique`/`not_null` cho khóa, `relationships` để bắt orphan record, cộng 5 test nghiệp vụ tự viết (quantity phải dương, discount trong khoảng hợp lệ, net_sales không được lớn hơn sales, v.v.).

---

## ⚙️ Airflow

DAG `ecommerce_elt_dag` chạy **9h sáng mỗi ngày (giờ VN)**, làm 2 việc theo thứ tự: `extract_load` (dlt) xong rồi mới tới `dbt_transform`. Mình dùng Cosmos để tách mỗi model + test thành task riêng — fail chỗ nào thấy ngay chỗ đó, retry đúng chỗ, khỏi mò.

![Airflow DAG Graph](docs/images/dag_graph.png)

---

## 📊 Dashboard

3 trang, cùng tông dark/tech. Nguyên tắc mình đặt ra: **mỗi trang chỉ trả lời vài câu hỏi cốt lõi**, không nhồi hết mọi con số vào một chỗ cho rối mắt.

- **Overview** — sức khỏe kinh doanh: doanh thu, lợi nhuận, đơn hàng, xu hướng theo tháng, theo quốc gia.
- **Product** — category nào gánh doanh thu, category nào đang lỗ, và discount có đang ăn mòn lợi nhuận không.
- **Customers** — khách là ai, khách mới hay khách quay lại nuôi doanh thu, bao nhiêu người mua đúng 1 lần rồi biến mất.

> 👉 **[Bấm vào đây để xem dashboard chạy trực tiếp](https://app.powerbi.com/view?r=eyJrIjoiOTAwYjNmMjYtZjRmNS00Y2I0LTgxMjYtZjYxYjNkNzhmZWRkIiwidCI6IjM3MGZiM2I4LTMzMDYtNDg5MC05MDYzLWNjMDhiZTc4ODI1NyIsImMiOjEwfQ%3D%3D)**

![Overview Dashboard](docs/images/dashboard_overview.png)
![Product Dashboard](docs/images/dashboard_product.png)
![Customers Dashboard](docs/images/dashboard_customers.png)

---

## 🚀 Chạy lại từ đầu

<details>
<summary><b>1. Extract + Load (dlt)</b></summary>

```bash
cd el
python -m venv venv && source venv/bin/activate
pip install -r ../requirements-el.txt
# điền credentials vào .dlt/secrets.toml (SQL Server + Snowflake)
python pipeline_ecommerce_v1_fullload.py
```
</details>

<details>
<summary><b>2. Transform (dbt)</b></summary>

```bash
cd dbt_ecommerce
python -m venv venv && source venv/bin/activate
pip install -r requirements-dbt.txt
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve   # xem lineage graph
```
</details>

<details>
<summary><b>3. Orchestration (Airflow + Docker)</b></summary>

```bash
curl -sSL install.astronomer.io | sudo bash -s
cd airflow
# điền credentials vào .env và airflow_settings.yaml
astro dev start   # UI ở localhost:8080
```
</details>

<details>
<summary><b>4. Power BI</b></summary>

Get Data → Snowflake → **Import mode** → chỉ chọn bảng `fct_*` và `dim_*` trong schema `dev`.
</details>

> ⚠️ Credentials (`.env`, `secrets.toml`, `airflow_settings.yaml`) đều nằm trong `.gitignore` — không có cái nào bị đẩy lên GitHub.

---

## <a name="what-i-learned"></a>🧠 What I learned

Phần này mới là phần đáng giá nhất của project. Mấy chỗ mình vấp và cách xử lý:

**`SALES` không phải doanh thu thật.** Ban đầu mình tưởng cột `sales` là tiền thu về, hóa ra nó là giá gốc *chưa trừ discount*. Phát hiện ra bằng cách so `unit_price` của cùng một sản phẩm ở các mức discount khác nhau — thấy nó không đổi. Từ đó mới tính riêng `net_sales = sales * (1 - discount)`. Nếu không để ý chỗ này thì mọi con số doanh thu trên dashboard đều sai mà không ai biết.

**Mã region/product bị bẩn.** Một số dòng có mã kiểu `RR0001` (lặp prefix). Không clean thì join với dimension gãy âm thầm — không báo lỗi, chỉ là số bị thiếu. Mình `REGEXP_REPLACE` ngay ở staging. Rủi ro đổi lại: lỡ nguồn có sẵn cả `R001` lẫn `RR001` thì sau khi clean sẽ đụng nhau — nên mình để test `relationships` làm chốt chặn cuối.

**Cosmos + dbt trong venv riêng = một cú lừa.** dbt mình cài trong venv tách biệt để khỏi đụng dependency của Airflow. Nhưng Cosmos mặc định giả định dbt nằm chung env, nên phải ép `InvocationMode.SUBPROCESS` ở **cả `ExecutionConfig` lẫn `RenderConfig`** — thiếu một trong hai là DAG fail ngay lúc parse. Cái này ngốn của mình kha khá thời gian mới ra.

**Bỏ RFM là quyết định đúng.** Lúc đầu định làm RFM scoring cho khách hàng cho "xịn", nhưng nghĩ lại thấy over-engineer so với scope. Thay bằng `fct_customer_summary` tổng hợp thô — đơn giản, đủ trả lời câu hỏi, không phải giải thích một đống logic gán điểm.

**`fct_customer_summary` không link được `dim_date`.** Bảng này grain là 1 dòng/khách (tổng hợp *cả đời* khách hàng), nên không có cột ngày để nối vào `dim_date`. Hệ quả: mấy chỉ số như "khách quay lại" không lọc được theo năm trong Power BI. Mình chấp nhận giới hạn này thay vì nhét ngày vào — vì làm vậy sẽ phá vỡ đúng ý nghĩa của cột. Trên dashboard mình ghi rõ "All-time" để người xem không hiểu nhầm.

---

## 📁 Cấu trúc repo

```
xom-ecommerce-data-pipeline/
├── el/                    # Extract + Load (dlt)
├── dbt_ecommerce/         # Transform — staging, intermediate, marts, tests
├── airflow/               # Orchestration (Astro project, Docker)
└── docs/
    ├── schema_dump.txt    # Khảo sát schema nguồn
    └── images/            # Screenshot DAG + dashboard
```

---

<p align="center"><sub>Built by trinpb04 · SQL Server → dlt → Snowflake → dbt → Airflow → Power BI</sub></p>
