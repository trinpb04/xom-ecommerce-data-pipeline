import dlt
from dlt.sources.sql_database import sql_database

CORE_TABLES = ["customer", "ecom_sales", "product", "region"]

def run_full_load():  #pipeline v1 - full load
    # schema="e_commerce": CHỈ quét schema này, bỏ qua các schema khác
    source = sql_database(schema="e_commerce").with_resources(*CORE_TABLES)

    pipeline = dlt.pipeline(
        pipeline_name="ecommerce_mssql_to_snowflake",
        destination="snowflake",
        dataset_name="e_commerce",   # = tên schema đích bên Snowflake
        progress="log",
    )

    # replace = xóa bảng cũ, ghi lại từ đầu
    info = pipeline.run(source, write_disposition="replace")
    print(info)

if __name__ == "__main__":
    run_full_load()