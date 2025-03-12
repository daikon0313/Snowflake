import json
from snowflake.snowpark.session import Session
from cryptography.hazmat.primitives import serialization
import streamlit as st

# JSONファイルを読み込む
with open("./connection.json") as f:
    snowflake_connection_cfg = json.load(f)

# 秘密鍵ファイルの読み込みとデコード
private_key_path = snowflake_connection_cfg["private_key"]
with open(private_key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None
    )

# Snowflakeに接続
snowflake_connection_cfg["private_key"] = private_key
session = Session.builder.configs(snowflake_connection_cfg).create()

# SQLファイルを読み込む関数
def load_sql(file_path):
    with open(file_path, "r") as file:
        return file.read()

# クエリ読み込み
unused_tables_query = load_sql("sql/unused_tables.sql")
unused_views_query = load_sql("sql/unused_views.sql")

st.title("未使用テーブル・ビューの特定")

# 未使用テーブル
st.subheader("使用されていないテーブル")
st.code(unused_tables_query, language="sql")

st.write("クエリを実行中...")
unused_tables_df = session.sql(unused_tables_query).collect()
st.dataframe(unused_tables_df)

# 未使用ビュー
st.subheader("使用されていないビュー")
st.code(unused_views_query, language="sql")

st.write("クエリを実行中...")
unused_views_df = session.sql(unused_views_query).collect()
st.dataframe(unused_views_df)