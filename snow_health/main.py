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

# テストクエリ実行
df = session.sql("SELECT 1 AS id").collect()
st.write(df)