import pandas as pd
import matplotlib.pyplot as plt

# Đọc file CSV
df = pd.read_csv('ElonMusk_tweets.csv')

# Chuyển đổi cột 'created_at' sang kiểu datetime
df['created_at'] = pd.to_datetime(df['created_at'])

# b. Đếm số tweets theo từng khung giờ
df['hour'] = df['created_at'].dt.hour
tweets_by_hour = df['hour'].value_counts().sort_index()

# In kết quả ra console
print("Số tweets theo từng khung giờ:")
print(tweets_by_hour)

# Xác định khung giờ mà Elon Musk thường đăng tweet nhiều nhất
most_common_hour = tweets_by_hour.idxmax()
print("Elon Musk thường đăng tweet vào khung giờ:", most_common_hour)

# Vẽ biểu đồ số tweets theo từng khung giờ
plt.figure(figsize=(12, 6))
tweets_by_hour.plot(kind='bar', color='salmon')
plt.title("Số Tweets Theo Từng Khung Giờ")
plt.xlabel("Giờ")
plt.ylabel("Số Tweet")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

# Lưu kết quả phân tích vào file txt
with open("tweet_by_hour_results.txt", "w", encoding="utf-8") as f:
    f.write("Số tweets theo từng khung giờ:\n")
    f.write(str(tweets_by_hour) + "\n\n")
    f.write("Elon Musk thường đăng tweet vào khung giờ: {}\n".format(most_common_hour))
