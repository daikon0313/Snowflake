import streamlit as st
from collections import defaultdict
from snowflake.core import Root
from snowflake.snowpark.context import get_active_session

FIXED_DATABASE = "INTERNAL_KPI_DEV"
FIXED_SCHEMA = "HARATO_TEST_SCHEMA"
SEARCH_SERVICE_NAME = "opportunity_history_search_service"

session = get_active_session()
root = Root(session)

st.title("🔍 プロジェクト履歴検索")

query = st.text_input(
    "検索キーワードを入力してください（例：原戸 Tableau ANA）",
    placeholder="社員名、クライアント名、ツールなどをスペースで区切って入力"
)

if query:
    cortex_search_service = root.databases[FIXED_DATABASE].schemas[FIXED_SCHEMA].cortex_search_services[SEARCH_SERVICE_NAME]

    results = cortex_search_service.search(
        query=query,
        columns=[
            "emp_name", "opportunity_history",
            "bd_project_code", "end_client_name", "opportunity_name", "opportunity_category",
            "analytics_tool_name", "bi_tool_name", "etl_tool_name", "ma_tool_name",
            "project_start_date", "project_end_date"
        ],
        limit=100
    )

    keywords = query.strip().split()
    st.subheader(f"🔎 『{'・'.join(keywords)}』をすべて含むプロジェクト履歴")

    if not results.results:
        st.info("該当するプロジェクトは見つかりませんでした。")
    else:
        # 社員 → 案件 → 明細 の入れ子 + 開始日つきで管理
        grouped = defaultdict(lambda: defaultdict(list))

        for r in results.results:
            emp_name = r.get("emp_name", "")
            opp_name = r.get("opportunity_name", "")
            client = r.get("end_client_name", "")
            category = r.get("opportunity_category", "")
            tools = "・".join([
                r.get("analytics_tool_name", "N/A"),
                r.get("bi_tool_name", "N/A"),
                r.get("etl_tool_name", "N/A"),
                r.get("ma_tool_name", "N/A")
            ])
            start_date = r.get("project_start_date", "")
            end_date = r.get("project_end_date", "")
            history = r.get("opportunity_history", "")

            text_all = f"{emp_name} {client} {opp_name} {category} {tools} {start_date} {end_date} {history}"
            if all(k in text_all for k in keywords):
                grouped[emp_name][opp_name].append({
                    "client": client,
                    "category": category,
                    "tools": tools,
                    "start_date": start_date,
                    "end_date": end_date,
                    "history": history
                })

        if not grouped:
            st.info("検索にはヒットしましたが、すべてのキーワードに一致する結果は見つかりませんでした。")
        else:
            for emp_name in sorted(grouped.keys()):
                st.markdown(f"#### 👤 社員名：{emp_name}")

                # 案件単位で project_start_date が最も早いものを基準にソート
                project_list = []
                for opp_name, records in grouped[emp_name].items():
                    # 最も早い start_date を取得
                    earliest_date = min(str(r["start_date"]) or "9999-12-31" for r in records)
                    project_list.append((earliest_date, opp_name, records))

                # 📅 昇順（古い順）にソート
                project_list.sort()

                for _, opp_name, records in project_list:
                    st.markdown(f"###### 📁 案件名：{opp_name}")
                    sorted_records = sorted(records, key=lambda r: str(r["start_date"]) or "")

                    for rec in sorted_records:
                        st.markdown(f"- 🏷️ カテゴリ：{rec['category']}")
                        st.markdown(f"  - 🏢 クライアント：{rec['client']}")
                        st.markdown(f"  - 🛠️ 使用ツール：{rec['tools']}")
                        st.markdown(f"  - 📅 期間：{rec['start_date']} 〜 {rec['end_date']}")
                    with st.expander("📄 案件説明（自然文）を表示"):
                        for rec in sorted_records:
                            st.markdown(f"- {rec['history']}")
                    st.markdown("---")