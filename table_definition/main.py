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
    # connection.json を読み込み
    with open("connection.json") as f:
        config = json.load(f)

    # 秘密鍵ファイルを読み込み、PEM形式から秘密鍵オブジェクトに変換する
    with open(config["private_key"], "rb") as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None)

    # Snowflake 接続に使えるよう、秘密鍵を DER 形式のバイト列に変換
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # Snowflake へ接続（キーペア認証）
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
        # DESC VIEW コマンドでビュー定義情報を取得
        cs.execute("DESC VIEW D_HARATO_DB.NOTION.NOTION_MART;")
        rows = cs.fetchall()

        # 必要な項目「カラム名」「データ型」「項目桁」を抽出
        data = []
        for row in rows:
            col_name = row[0]
            full_type = row[1]  # 例: "VARCHAR(16777216)" や "NUMBER(38,1)" など

            # 正規表現でデータ型の基本部分と括弧内の桁/精度を抽出
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
    # ファイルパスの設定（すべて同じ階層にある前提）
    yaml_file = "notion_mart.yml"               # dbt モデルの YAML ファイル
    output_excel_file = "merged_view_definition.xlsx"  # 補完後の出力先

    # データベース名とスキーマ名を指定
    database_name = "D_HARATO_DB"
    schema_name = "NOTION"

    # YAML ファイルを読み込む
    with open(yaml_file, "r", encoding="utf-8") as f:
        model_yaml = yaml.safe_load(f)

    # YAML 内の対象モデル「notion_mart」の meta 情報、materialized、各カラムの description を抽出
    yaml_descriptions = {}
    meta_info = {"grade": "", "status": "", "target_user": "", "materialized": ""}
    for model in model_yaml.get("models", []):
        # モデル名は小文字にして比較
        if model.get("name", "").lower() == "notion_mart":
            # materialized がモデルレベルにある場合（存在しなければ空文字）
            meta_info["materialized"] = model.get("materialized", "")
            meta = model.get("meta", {})
            meta_info["grade"] = meta.get("grade", "")
            meta_info["status"] = meta.get("status", "")
            meta_info["target_user"] = meta.get("target_user", "")
            for col in model.get("columns", []):
                yaml_descriptions[col["name"]] = col.get("description", "")

    # YAML の description を、もとのカラム名をキーにして補完
    df["description"] = df["カラム名"].map(yaml_descriptions).fillna("")

    # カラム名はそのまま保持
    df["カラム名"] = df["カラム名"].astype(str)

    # データベース名とスキーマ名の列を追加
    df["データベース名"] = database_name
    df["スキーマ名"] = schema_name

    # meta 情報を各行に追加
    df["grade"] = meta_info["grade"]
    df["status"] = meta_info["status"]
    df["target_user"] = meta_info["target_user"]
    # materialized 情報はモデルごとに同じ値として各行に追加
    df["materialized"] = meta_info["materialized"]

    # 列の最後に追加する空欄の列を作成
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

    # 既存の出力先ファイルがあれば削除して出力
    if os.path.exists(output_excel_file):
        os.remove(output_excel_file)
    df.to_excel(output_excel_file, index=False)
    print(f"マージされたビュー定義が '{output_excel_file}' に出力されました。")

if __name__ == "__main__":
    # 1. Snowflake からビュー定義を取得し、DataFrame として返す
    df_view = fetch_view_definition()

    # 2. YAML ファイルの情報を補完し、データベース名・スキーマ名および meta、materialized 情報の列を先頭に持たせ、
    #    列の最後に空欄の「サンプル値」「SPUっチェック」「仮名加工」「仮名加工処理」を追加して Excel ファイルを出力
    merge_with_yaml(df_view)