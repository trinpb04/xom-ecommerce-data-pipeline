#chỉ cần dùng khi dùng data real time update mới

import dlt
from dlt.sources.sql_database import sql_table

def run_incremental():
    orders = sql_table(
        table="ecom_sales",
        schema="e_commerce",             # vẫn phải chỉ đích danh schema
        incremental=dlt.sources.incremental(
            "updated_at",                # tên cột mốc từ khảo sát Phase 1
            initial_value="2020-01-01",
        ),
    )

    pipeline = dlt.pipeline(
        pipeline_name="ecommerce_mssql_to_snowflake",
        destination="snowflake",
        dataset_name="e_commerce",
    )

    # merge = upsert: mới thì insert, cũ thay đổi thì update theo primary key
    info = pipeline.run(orders, write_disposition="merge", primary_key="order_id")
    print(info)

if __name__ == "__main__":
    run_incremental()