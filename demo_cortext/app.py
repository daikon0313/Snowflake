import streamlit as st
import json
import requests
import os
import logging
import pandas as pd
import snowflake.connector
from typing import List, Optional
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# ログ設計
load_dotenv(override=True)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("app.log")]
)
logger = logging.getLogger(__name__)

# Snowflake 認証情報
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_PRIVATE_KEY = os.getenv("SNOWFLAKE_PRIVATE_KEY")
PRIVATE_KEY_PASSPHRASE = os.getenv("PRIVATE_KEY_PASSPHRASE", "")
HOST = os.getenv("SNOWFLAKE_HOST_URL")

DATABASE_CORTEX = "d_harato_db"
SCHEMA_CORTEX = "harato_sample_cortex_demo"
STAGE_CORTEX = "demo_stage"
FILE_CORTEX = "cust_info.yml"
WAREHOUSE_CORTEX = "d_harato_wh"

# RSA秘密鍵の読み込みと DER 形式への変換
private_key_obj = serialization.load_pem_private_key(
    SNOWFLAKE_PRIVATE_KEY.encode(),
    password=PRIVATE_KEY_PASSPHRASE.encode() if PRIVATE_KEY_PASSPHRASE else None,
    backend=default_backend()
)
private_key = private_key_obj.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

st.session_state.conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    account=SNOWFLAKE_ACCOUNT,
    private_key=private_key,
    warehouse=WAREHOUSE_CORTEX,
    database=DATABASE_CORTEX,
    schema=SCHEMA_CORTEX,
    role=SNOWFLAKE_ROLE
)

##################### 
def send_message(prompt: str) -> dict:
    """Cortex Analyst REST API を呼び出しレスポンスを返す"""
    body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "semantic_model_file": f"@{DATABASE_CORTEX}.{SCHEMA_CORTEX}.{STAGE_CORTEX}/{FILE_CORTEX}",
    }
    resp = requests.post(
        url=f"https://{HOST}/api/v2/cortex/analyst/message",
        json=body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
    )
    req_id = resp.headers.get("X-Snowflake-Request-Id")
    if resp.status_code < 400:
        return {**resp.json(), "request_id": req_id}
    else:
        raise Exception(f"Failed request (id: {req_id}) with status {resp.status_code}: {resp.text}")

def process_message(prompt: str) -> None:
    """ユーザー入力を表示し、API 呼び出し後の結果を表示"""
    st.session_state.messages.append({"role": "user", "content": [{"type": "text", "text": prompt}]})
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            response = send_message(prompt)
            req_id = response["request_id"]
            content = response["message"]["content"]
            display_content(content, req_id)
    st.session_state.messages.append({"role": "assistant", "content": content, "request_id": req_id})

def display_content(content: List[dict], req_id: Optional[str] = None, msg_index: Optional[int] = None) -> None:
    """API レスポンスの内容を表示"""
    msg_index = msg_index or len(st.session_state.messages)
    if req_id:
        with st.expander("Request ID", expanded=False):
            st.markdown(req_id)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for idx, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{msg_index}_{idx}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    df = pd.read_sql(item["statement"], st.session_state.CONN)
                    # チャート部分は省略し、テーブルのみ表示
                    st.dataframe(df)

# UI の初期化
st.title("Cortex Analyst Demo App")
st.markdown(f"Semantic Model: `{FILE_CORTEX}`")
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None

for i, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        display_content(msg["content"], msg.get("request_id"), i)

if user_input := st.chat_input("What is your question?"):
    process_message(user_input)

if st.session_state.active_suggestion:
    process_message(st.session_state.active_suggestion)
    st.session_state.active_suggestion = None
    