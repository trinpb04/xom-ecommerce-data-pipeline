import os
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

load_dotenv()  #Đọc file .env

SCHEMA = "e_commerce"  #Đích danh schema cần phân tích

conn_str = (
    f"mssql+pyodbc://{os.getenv('MSSQL_USER')}:{os.getenv('MSSQL_PASS')}"
    f"@{os.getenv('MSSQL_HOST')}:{os.getenv('MSSQL_PORT')}/{os.getenv('MSSQL_DB')}"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)

engine = create_engine(conn_str)
inspector = inspect(engine)

# Nếu không truyền schema=, SQLAlchemy chỉ nhìn schema mặc định (dbo) --> server trống trơn
for table in inspector.get_table_names(schema=SCHEMA):
    cols = inspector.get_columns(table, schema=SCHEMA)
    pk = inspector.get_pk_constraint(table, schema=SCHEMA)
    fks = inspector.get_foreign_keys(table, schema=SCHEMA)

    print(f"\n{'='*60}")
    print(f"{SCHEMA}.{table}  ({len(cols)} cột)")
    print(f" PK: {pk.get('constrained_columns')}")
    for fk in fks:
        print(f"  FK: {fk['constrained_columns']} -> {fk['referred_table']}.{fk['referred_columns']}")
    for c in cols:
        print(f"  {c['name']:<30} {c['type']}")