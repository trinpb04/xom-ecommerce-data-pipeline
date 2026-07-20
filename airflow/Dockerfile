FROM astrocrpublic.azurecr.io/runtime:3.3-2

# ODBC Driver 18 cho pyodbc (dlt doc SQL Server qua mssql+pyodbc)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl gnupg2 \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc \
    && rm -rf /var/lib/apt/lists/*
USER astro

# venv rieng cho dbt - tranh conflict dependency voi Airflow.
# Goi thang dbt_venv/bin/pip, khong can source/activate (moi RUN la shell moi)
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir dbt-snowflake
