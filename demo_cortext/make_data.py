import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# 乱数の種を固定（再現性のため）
random.seed(42)
np.random.seed(42)

# レコード数の設定
n_join = 7000   # 入会レコード数
n_leave = 3000  # 退会レコード数

# 利用するブランドと都道府県の例
brands = ['A', 'B', 'C']
prefectures = ['東京都', '大阪府', '神奈川県', '愛知県', '京都府', '北海道', '福岡県']

# 入会レコードの生成
join_records = []
for i in range(n_join):
    # 契約id、個人idは "CT00001" のように連番で付与
    contract_id = f"CT{i+1:05d}"
    personal_id = contract_id  # 同じ値を使用
    brand = random.choice(brands)
    prefecture = random.choice(prefectures)
    # 入会日は2024-01-01～2024-06-30の間（ランダム）
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 6, 30)
    delta_days = (end_date - start_date).days
    join_date = start_date + timedelta(days=random.randint(0, delta_days))
    
    join_records.append({
        "日付": join_date.strftime("%Y-%m-%d"),
        "契約id": contract_id,
        "個人id": personal_id,
        "ブランド": brand,
        "都道府県": prefecture,
        "入会フラグ": 1,
        "退会フラグ": 0
    })

# 退会レコードの生成
# 退会イベントを発生させる個人は join_records からランダムに選ぶ（重複なし）
leave_indices = np.random.choice(n_join, size=n_leave, replace=False)
leave_records = []
for idx in leave_indices:
    join_rec = join_records[idx]
    join_date = datetime.strptime(join_rec["日付"], "%Y-%m-%d")
    # 退会日は、必ず入会日の翌日以降かつ2024-07-01～2024-12-31の間
    leave_start = max(join_date + timedelta(days=1), datetime(2024, 7, 1))
    leave_end = datetime(2024, 12, 31)
    if leave_start > leave_end:
        # 万が一入会日が遅すぎる場合は退会レコードは発生させない（ここではスキップ）
        continue
    delta_leave = (leave_end - leave_start).days
    leave_date = leave_start + timedelta(days=random.randint(0, delta_leave))
    
    leave_records.append({
        "日付": leave_date.strftime("%Y-%m-%d"),
        "契約id": join_rec["契約id"],
        "個人id": join_rec["個人id"],
        "ブランド": join_rec["ブランド"],
        "都道府県": join_rec["都道府県"],
        "入会フラグ": 0,
        "退会フラグ": 1
    })

# 全レコードの結合
all_records = join_records + leave_records
# 日付順にソート
df = pd.DataFrame(all_records)
df["日付"] = pd.to_datetime(df["日付"])
df = df.sort_values("日付").reset_index(drop=True)

# 全体件数確認
print(f"全体件数: {len(df)}")  # およそ10000件になるはず

# 契約者数（累積入会 - 累積退会）の推移確認（オプション）
df = df.sort_values("日付")

# CSV として保存する場合（必要なら）
# df.to_csv("sample_contract_data.csv", index=False, date_format="%Y-%m-%d")

# データの先頭部分を表示（確認用）
print(df.head(10))

df.to_csv("sample_contract_data.csv", index=False, date_format="%Y-%m-%d")