import os
import re
import json
import yaml
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization

def fetch_view_definition():
    """
    Snowflake から対象ビュー（D_HARATO_DB.NOTION.NOTION_MART）の定義情報を取得し、
    「カラム名」「データ型」「項目桁」のみを抽出した DataFrame を返す。
    """

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
    """
    YAML ファイル（notion_mart.yml）からカラムごとの description と meta 情報、および materialized の情報を読み込み、
    DataFrame の「カラム名」とマッピングし、さらに「データベース名」「スキーマ名」、meta 情報、および materialized の列を追加した上で、
    列の最後に「サンプル値」「SPUっチェック」「仮名加工」「仮名加工処理」の列（空欄）を追加し、
    最終的に列順を調整して Excel ファイルに出力する。
    """

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

    df["サンプル値"] = ""
    df["SPUっチェック"] = ""
    df["仮名加工"] = ""
    df["仮名加工処理"] = ""

    # 列の順序を調整:
    # A列：materialized、B列：grade、C列：status、D列：target_user、
    # E列：データベース名、F列：スキーマ名、G列：カラム名、H列：データ型、I列：項目桁、J列：description、
    # K列：サンプル値、L列：SPUっチェック、M列：仮名加工、N列：仮名加工処理
    desired_columns = [
        "materialized", "grade", "status", "target_user",
        "データベース名", "スキーマ名", "カラム名", "データ型", "項目桁", "description",
        "サンプル値", "SPUっチェック", "仮名加工", "仮名加工処理"
    ]
    df = df[desired_columns]

    if os.path.exists(output_excel_file):
        os.remove(output_excel_file)
    df.to_excel(output_excel_file, index=False)
    print(f"マージされたビュー定義が '{output_excel_file}' に出力されました。")

if __name__ == "__main__":
    df_view = fetch_view_definition()
    merge_with_yaml(df_view)