import pandas as pd
import matplotlib.pyplot as plt

# Đọc file CSV
df = pd.read_csv('ElonMusk_tweets.csv')

# Chuyển đổi cột 'created_at' sang kiểu datetime
df['created_at'] = pd.to_datetime(df['created_at'])

# a. Đếm số tweets của từng ngày
df['date'] = df['created_at'].dt.date
tweets_by_day = df['date'].value_counts().sort_index()

# In kết quả đầy đủ ra console
print("Số tweets theo từng ngày:")
print(tweets_by_day.to_string())

# Vẽ biểu đồ số tweets theo từng ngày
plt.figure(figsize=(14, 7))
tweets_by_day.plot(kind='bar', color='skyblue')
plt.title("Số Tweets Theo Từng Ngày", fontsize=16)
plt.xlabel("Ngày", fontsize=14)
plt.ylabel("Số Tweet", fontsize=14)
plt.xticks(rotation=45, fontsize=10)
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()

# Lưu kết quả phân tích vào file txt
with open("tweet_by_day_results.txt", "w", encoding="utf-8") as f:
    f.write("Số tweets theo từng ngày:\n")
    f.write(tweets_by_day.to_string())
