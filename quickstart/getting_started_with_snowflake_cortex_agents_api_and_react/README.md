[Getting Started with Snowflake Cortex Agents API and React](https://quickstarts.snowflake.com/guide/getting_started_with_snowflake_agents_api_and_react/index.html?index=../..index#0)を行う

## 概要

このクイックスタートでは、Snowflake Cortex Agents を活用して、構造化データと非構造化データの両方を横断的に連携・活用し、インサイトを引き出すアプリケーションの構築方法を学べます。

Cortex Agents は以下を統合して動作します：
• Cortex Analyst：構造化データ向けのクエリ実行
• Cortex Search：非構造化データのインサイト取得
• LLM（大規模言語モデル）：自然言語理解と応答生成

## 学べること
• Cortex Agents API を使ったアプリケーション開発
• Cortex Analyst を活用した構造化データのクエリ実行
• Cortex Search を使った非構造化データの検索・分析
• キーペア認証方式の実装方法
• ツールリソースと仕様の取り扱い
• API応答の処理・管理のベストプラクティス

## 構築するアプリケーション

Next.js を用いて構築された完全動作するチャットボットUI付きアプリ。自然言語で質問を投げかけることで、以下を実現：
• Cortex Analyst / Search への自動ルーティング
• 構造化・非構造化データ両方へのアクセス
• 必要に応じたツールの利用（タスク実行）
• Snowflake内でのセキュアな処理
• LLMによる応答生成（テキスト・チャート・テーブル・出典付き）
