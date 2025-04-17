import streamlit as st
import json
import _snowflake
from snowflake.snowpark.context import get_active_session

session = get_active_session()

API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000  # in milliseconds

CORTEX_SEARCH_SERVICES = "d_harato_db.cortex_agent.sales_conversation_search"
SEMANTIC_MODELS = "@d_harato_db.cortex_agent.models/sales_metrics_model.yml"

def run_snowflake_query(query):
    try:
        df = session.sql(query.replace(';',''))
        
        return df

    except Exception as e:
        st.error(f"Error executing SQL: {str(e)}")
        return None, None

def snowflake_api_call(query: str, limit: int = 10):
    
    payload = {
        "model": "llama3.1-70b",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": query
                    }
                ]
            }
        ],
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "analyst1"
                }
            },
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": "search1"
                }
            }
        ],
        "tool_resources": {
            "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
            "search1": {
                "name": CORTEX_SEARCH_SERVICES,
                "max_results": limit,
                "id_column": "conversation_id"
            }
        }
    }
    
    try:
        resp = _snowflake.send_snow_api_request(
            "POST",  # method
            API_ENDPOINT,  # path
            {},  # headers
            {},  # params
            payload,  # body
            None,  # request_guid
            API_TIMEOUT,  # timeout in milliseconds,
        )
        
        if resp["status"] != 200:
            st.error(f"❌ HTTP Error: {resp['status']} - {resp.get('reason', 'Unknown reason')}")
            st.error(f"Response details: {resp}")
            return None
        
        try:
            response_content = json.loads(resp["content"])
        except json.JSONDecodeError:
            st.error("❌ Failed to parse API response. The server may have returned an invalid JSON format.")
            st.error(f"Raw response: {resp['content'][:200]}...")
            return None
            
        return response_content
            
    except Exception as e:
        st.error(f"Error making request: {str(e)}")
        return None

def process_sse_response(response):
    """Process SSE response with Streamlit debug output"""
    text = ""
    sql = ""
    citations = []

    if not response:
        st.warning("🔍 response is empty or None.")
        return text, sql, citations
    if isinstance(response, str):
        st.warning("🔍 response is a plain string, not a structured list.")
        st.text(response)
        return text, sql, citations

    try:
        st.info("📥 Starting to process response events...")
        for i, event in enumerate(response):
            st.write(f"🧩 Event #{i}:")
            st.json(event)

            if event.get('event') == "message.delta":
                data = event.get('data', {})
                delta = data.get('delta', {})

                st.write(f"🔄 Delta for Event #{i}:")
                st.json(delta)

                for content_item in delta.get('content', []):
                    content_type = content_item.get('type')
                    st.write(f"🔍 Content item type: `{content_type}`")
                    st.json(content_item)

                    if content_type == "tool_results":
                        tool_results = content_item.get('tool_results', {})
                        if 'content' in tool_results:
                            for result in tool_results['content']:
                                if result.get('type') == 'json':
                                    json_data = result.get('json', {})
                                    st.write("📦 tool_results → json:")
                                    st.json(json_data)

                                    text_part = json_data.get('text', '')
                                    text += text_part
                                    if text_part:
                                        st.success(f"📄 Appended text: {text_part}")

                                    search_results = json_data.get('searchResults', [])
                                    for search_result in search_results:
                                        st.write("🔗 Search result:")
                                        st.json(search_result)
                                        citations.append({
                                            'source_id': search_result.get('source_id', ''),
                                            'doc_id': search_result.get('doc_id', '')
                                        })

                                    sql_candidate = json_data.get('sql', '')
                                    if sql_candidate:
                                        st.code(sql_candidate, language='sql')
                                        sql = sql_candidate

                    elif content_type == 'text':
                        text_part = content_item.get('text', '')
                        text += text_part
                        if text_part:
                            st.success(f"💬 Appended text: {text_part}")

    except json.JSONDecodeError as e:
        st.error(f"❌ JSON decoding error: {str(e)}")

    except Exception as e:
        st.error(f"❌ Unexpected error: {str(e)}")

    st.info("✅ SSE Response processing complete.")
    return text, sql, citations
def main():
    st.title("Intelligent Sales Assistant")

    # Sidebar for new chat
    with st.sidebar:
        if st.button("New Conversation", key="new_chat"):
            st.session_state.messages = []
            st.rerun()

    # Initialize session state
    if 'messages' not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message['role']):
            st.markdown(message['content'].replace("•", "\n\n"))

    if query := st.chat_input("Would you like to learn?"):
        # Add user message to chat
        with st.chat_message("user"):
            st.markdown(query)
        st.session_state.messages.append({"role": "user", "content": query})
        
        # Get response from API
        with st.spinner("Processing your request..."):
            response = snowflake_api_call(query, 1)
            text, sql, citations = process_sse_response(response)
            
            # Add assistant response to chat
            if text:
                text = text.replace("【†", "[")
                text = text.replace("†】", "]")
                st.session_state.messages.append({"role": "assistant", "content": text})
                
                with st.chat_message("assistant"):
                    st.markdown(text.replace("•", "\n\n"))
                    if citations:
                        st.write("Citations:")
                        for citation in citations:
                            doc_id = citation.get("doc_id", "")
                            if doc_id:
                                query = f"SELECT transcript_text FROM sales_conversations WHERE conversation_id = '{doc_id}'"
                                result = run_snowflake_query(query)
                                result_df = result.to_pandas()
                                if not result_df.empty:
                                    transcript_text = result_df.iloc[0, 0]
                                else:
                                    transcript_text = "No transcript available"
                    
                                with st.expander(f"[{citation.get('source_id', '')}]"):
                                    st.write(transcript_text)

            # Display SQL if present
            if sql:
                st.markdown("### Generated SQL")
                st.code(sql, language="sql")
                sales_results = run_snowflake_query(sql)
                if sales_results:
                    st.write("### Sales Metrics Report")
                    st.dataframe(sales_results)

if __name__ == "__main__":
    main()