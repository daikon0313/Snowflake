from typing import Dict, List, Optional

import _snowflake
import json
import streamlit as st
from snowflake.snowpark.context import get_active_session

# あなたのステージの設定
DATABASE = "INTERNAL_KPI_DEV"
SCHEMA = "HARATO_TEST_SCHEMA"
STAGE = "PUBLIC"
FILE = "sample.yml"

def send_message(prompt: str) -> dict:
    """Calls the Cortex Analyst API and returns the response."""
    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
    }

    resp = _snowflake.send_snow_api_request(
        "POST",
        f"/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )

    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        st.session_state.messages.pop()
        raise Exception(
            f"Failed request with status {resp['status']}: {resp}"
        )

def process_message(prompt: str) -> None:
    """Handles user input and appends response."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("考え中..."):
            response = send_message(prompt=prompt)
            request_id = response["request_id"]
            content = response["message"]["content"]
            st.session_state.messages.append(
                {**response['message'], "request_id": request_id}
            )
            display_content(content=content, request_id=request_id)

def display_content(
    content: List[Dict[str, str]],
    request_id: Optional[str] = None,
    message_index: Optional[int] = None,
) -> None:
    """Displays content from a message."""
    message_index = message_index or len(st.session_state.messages)
    if request_id:
        with st.expander("Request ID", expanded=False):
            st.markdown(request_id)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            display_sql(item["statement"])

@st.cache_data
def display_sql(sql: str) -> None:
    with st.expander("SQLクエリ", expanded=False):
        st.code(sql, language="sql")
    with st.expander("結果", expanded=True):
        with st.spinner("SQLを実行中..."):
            session = get_active_session()
            df = session.sql(sql).to_pandas()
            if len(df.index) > 1:
                data_tab, line_tab, bar_tab = st.tabs(["データ", "折れ線グラフ", "棒グラフ"])
                data_tab.dataframe(df)
                if len(df.columns) > 1:
                    df = df.set_index(df.columns[0])
                with line_tab:
                    st.line_chart(df)
                with bar_tab:
                    st.bar_chart(df)
            else:
                st.dataframe(df)

def show_conversation_history() -> None:
    for message_index, message in enumerate(st.session_state.messages):
        chat_role = "assistant" if message["role"] == "analyst" else "user"
        with st.chat_message(chat_role):
            display_content(
                content=message["content"],
                request_id=message.get("request_id"),
                message_index=message_index,
            )

def reset() -> None:
    st.session_state.messages = []
    st.session_state.suggestions = []
    st.session_state.active_suggestion = None

st.title("📊 プロジェクト履歴アナリスト（Cortex Analyst）")
st.markdown(f"使用中のセマンティックモデル: `{FILE}`")

if "messages" not in st.session_state:
    reset()

with st.sidebar:
    if st.button("会話をリセット"):
        reset()

show_conversation_history()

if user_input := st.chat_input("ご質問を入力してください（例：原戸さんが関わった案件は？）"):
    process_message(prompt=user_input)

if st.session_state.active_suggestion:
    process_message(prompt=st.session_state.active_suggestion)
    st.session_state.active_suggestion = None