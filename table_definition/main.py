import os
import re
import json
import yaml
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization

def get_snowflake_connection():

    with open("connection.json") as f:
        config = json.load(f)

    with open(config["private_key"], "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None)

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    conn = snowflake.connector.connect(
        account=config["account"],
        user=config["user"],
        private_key=private_key_bytes,
        warehouse=config["warehouse"],
        database=config["database"],
        schema=config["schema"],
        role=config["role"]
    )
    return conn

def fetch_view_definition():

    conn = get_snowflake_connection()
    try:
        cs = conn.cursor()
        cs.execute("DESC VIEW D_HARATO_DB.NOTION.NOTION_MART;")
        rows = cs.fetchall()
        data = []
        for row in rows:
            col_name = row[0]
            full_type = row[1]
            match = re.match(r"^([^(]+)(?:\(([^)]+)\))?", full_type)
            if match:
                base_type = match.group(1).strip()
                length = match.group(2).strip() if match.group(2) else ""
            else:
                base_type = full_type
                length = ""
            data.append({
                "カラム名": col_name,
                "データ型": base_type,
                "項目桁": length
            })
        df = pd.DataFrame(data)
        return df
    finally:
        cs.close()
        conn.close()

def merge_with_yaml(df):

    yaml_file = "notion_mart.yml"
    output_excel_file = "merged_view_definition.xlsx"

    database_name = "D_HARATO_DB"
    schema_name = "NOTION"

    with open(yaml_file, "r", encoding="utf-8") as f:
        model_yaml = yaml.safe_load(f)

    yaml_descriptions = {}
    meta_info = {"grade": "", "status": "", "target_user": "", "materialized": ""}
    for model in model_yaml.get("models", []):
        if model.get("name", "").lower() == "notion_mart":
            meta_info["materialized"] = model.get("materialized", "")
            meta = model.get("meta", {})
            meta_info["grade"] = meta.get("grade", "")
            meta_info["status"] = meta.get("status", "")
            meta_info["target_user"] = meta.get("target_user", "")
            for col in model.get("columns", []):
                yaml_descriptions[col["name"]] = col.get("description", "")

    df["description"] = df["カラム名"].map(yaml_descriptions).fillna("")
    df["カラム名"] = df["カラム名"].astype(str)
    df["データベース名"] = database_name
    df["スキーマ名"] = schema_name
    df["grade"] = meta_info["grade"]
    df["status"] = meta_info["status"]
    df["target_user"] = meta_info["target_user"]
    df["materialized"] = meta_info["materialized"]

    conn = get_snowflake_connection()
    sample_values = {}
    try:
        for col in df["カラム名"]:
            cs = conn.cursor()
            try:
                query = f"SELECT CAST({col} AS VARCHAR) FROM D_HARATO_DB.NOTION.NOTION_MART WHERE {col} IS NOT NULL LIMIT 1"
                cs.execute(query)
                result = cs.fetchone()
                sample_values[col] = result[0] if result is not None else ""
            except Exception as e:
                sample_values[col] = ""
            finally:
                cs.close()
    finally:
        conn.close()
    df["サンプル値"] = df["カラム名"].map(sample_values)

    df["SPUっチェック"] = ""
    df["仮名加工"] = ""
    df["仮名加工処理"] = ""

    desired_columns = [
        "materialized", "grade", "status", "target_user",
        "データベース名", "スキーマ名", "カラム名", "データ型", "項目桁", "description",
        "サンプル値", "SPUっチェック", "仮名加工", "仮名加工処理"
    ]
    df = df[desired_columns]

    if os.path.exists(output_excel_file):
        os.remove(output_excel_file)
    df.to_excel(output_excel_file, index=False)
    print(f"Merged view definition has been output to '{output_excel_file}'.")

if __name__ == "__main__":
    df_view = fetch_view_definition()
    merge_with_yaml(df_view)