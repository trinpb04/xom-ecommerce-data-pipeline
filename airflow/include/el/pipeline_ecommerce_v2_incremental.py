#chỉ cần dùng khi data nguồn có dòng mới - phase hiện tại vẫn chạy v1 full load

import dlt
from datetime import date
from dlt.sources.sql_database import sql_table

def run_incremental():
    # ecom_sales KHÔNG có cột updated_at (xem docs/schema_dump.txt - chỉ 11 cột)
    # -> mốc incremental duy nhất dùng được là order_date
    orders = sql_table(
        table="ecom_sales",
        schema="e_commerce",             # vẫn phải chỉ đích danh schema
        incremental=dlt.sources.incremental(
            "order_date",
            initial_value=date(2018, 1, 1),  # order_date là kiểu DATE -> mốc phải là date, không phải string
        ),
    )

    pipeline = dlt.pipeline(
        pipeline_name="ecommerce_mssql_to_snowflake",
        destination="snowflake",
        dataset_name="e_commerce",
    )

    # merge = upsert theo primary key.
    # PHẢI là row_id (grain = 1 dòng/sản phẩm trong đơn) - nếu merge theo order_id
    # thì 1 đơn nhiều sản phẩm sẽ bị đè còn đúng 1 dòng -> mất data
    info = pipeline.run(orders, write_disposition="merge", primary_key="row_id")
    print(info)

if __name__ == "__main__":
    run_incremental()
