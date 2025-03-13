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

# Streamlit UI
st.title("未使用テーブル・ビューの特定")

# 日数選択（30, 90, 180, 360日）
days_option = st.radio(
    "未使用の期間（日数）を選択してください",
    [30, 90, 180, 360],
    index=0
)

st.write(f"現在の設定: **{days_option}日** 未使用のテーブル・ビューを表示")

# クエリ読み込み & 置換
unused_tables_query = load_sql("sql/unused_tables.sql").replace("current_date - 30", f"current_date - {days_option}")
unused_views_query = load_sql("sql/unused_views.sql").replace("current_date - 30", f"current_date - {days_option}")

# 未使用テーブルの取得
unused_tables_df = session.sql(unused_tables_query).collect()
unused_tables_count = len(unused_tables_df)

# 未使用ビューの取得
unused_views_df = session.sql(unused_views_query).collect()
unused_views_count = len(unused_views_df)

# 未使用テーブル
st.subheader("使用されていないテーブル")
st.markdown(f"**該当オブジェクト数: {unused_tables_count} 件**")
st.dataframe(unused_tables_df)

# 未使用ビュー
st.subheader("使用されていないビュー")
st.markdown(f"**該当オブジェクト数: {unused_views_count} 件**")
st.dataframe(unused_views_df)