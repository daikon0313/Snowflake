https://quickstarts.snowflake.com/guide/getting_started_with_cortex_agents/index.html?index=../..index#0

📌 Overview

現代の組織は、**構造化データ（KPIや売上指標）と非構造化データ（顧客会話、メール、議事録など）**の両方を活用する必要があります。本チュートリアルでは、Snowflake Cortex の機能を活用して、**セールス会話と指標を分析できる「Intelligent Sales Assistant」**を構築します。

⸻

❓ What is Snowflake Cortex?

Snowflake Cortex には以下の3つの主要機能があります：

🧮 Cortex Analyst
• 自然言語 → SQL 変換を実行
• YAMLで定義されたセマンティックモデルを理解
• SQLを書くことなくデータを分析
• 複雑な分析質問にも対応（精度90％超）
• ビジネス文脈を含んだ質問にも柔軟対応

🔍 Cortex Search
• セマンティック＋キーワード検索のハイブリッド手法
• Embeddingモデル（E5）により文脈の類似度を高精度に解析
• 大規模テキストデータのリアルタイム検索が可能
• 関連性スコアに基づいた結果ランキング

🤖 Cortex Agents
• REST API によるシームレスなインターフェース
• Cortex Analyst + Search を統合
• 機能：
• 文脈の検索（セマンティック＆キーワード）
• 自然言語→SQL変換
• LLMの制御・プロンプト管理
• 特徴：
• 出典付きの回答
• 無関係な質問は「回答控え」
• 会話履歴の管理
• 単一API呼び出しで完結
• リアルタイム応答用のストリーミング対応

⸻

🎯 これらの機能を通じてできること
• セールス会話から文脈を検索
• テキスト → SQL により売上分析を実行
• 構造化・非構造化データを組み合わせて分析
• データに対して自然言語で質問しインタラクティブに利用

⸻

📘 What You’ll Learn（学べること）
• セールスインテリジェンス用の Snowflake データベース構築
• Cortex Search サービスの作成・設定
• Streamlit でセールス分析 UI を構築
• 会話検索のセマンティック検索を実装
• LLMを使ったQAシステムの構築

⸻

🏗 What You’ll Build（構築するアプリ）
• セールス会話を意味的に検索できるアプリ
• 売上パターンを分析
• 自然言語で売上に関する質問をし、AI応答を得られるアシスタント

⸻

🔧 What You’ll Need（前提条件）
• Snowflake アカウント
• データベース、スキーマ、テーブル作成・ファイルアップロードの権限
• RSA 公開鍵の設定が必要 → Snowflake UI
• Cortex 機能へのアクセス
• Cortex Agent, Search, Analyst すべてにアクセス可能であること