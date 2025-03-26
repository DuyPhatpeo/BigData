# Hướng Dẫn Xử Lý Dữ Liệu ElonMusk_tweets.csv

_(Sử dụng Hadoop MapReduce & Apache Spark với trực quan hóa dữ liệu)_

File này hướng dẫn các bước sau:

- Tạo các file Python (mapper, reducer) bằng nano.
- Upload file dữ liệu lên HDFS.
- Chạy job MapReduce đếm số tweet theo ngày và theo khung giờ.
- Xử lý dữ liệu bằng Apache Spark và trực quan hóa kết quả bằng biểu đồ.

> **Ghi chú:** Các bước cài đặt Java, Hadoop, Spark đã được thực hiện.  
> Tài khoản HDFS được sử dụng: **hdoop**.

---

## Phần I: Hadoop MapReduce

### 1. Tạo các file Python cho MapReduce

#### 1.1 Tạo file `mapper_date.py`

Mở terminal và nhập:

```bash
nano mapper_date.py
```

Sau đó, dán nội dung sau:

```bash
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    fields = line.split()
    # Dữ liệu có ít nhất 4 trường: id, date, time, text
    if len(fields) < 4:
        continue
    date = fields[1]  # Trường thứ 2 là ngày
    print(f"{date}\t1")
```

Lưu file bằng cách nhấn Ctrl+X, sau đó Y và Enter.

1.2 Tạo file reducer_date.py
Mở terminal:

```bash
nano reducer_date.py
```

Dán nội dung sau:

```bash
import sys

current_date = None
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    date, value = line.split("\t")
    value = int(value)

    if current_date == date:
        count += value
    else:
        if current_date is not None:
            print(f"{current_date}\t{count}")
        current_date = date
        count = value

if current_date is not None:
    print(f"{current_date}\t{count}")
```

Lưu file.

1.3 Tạo file mapper_hour.py
Mở terminal:

```bash
nano mapper_hour.py
```

Dán nội dung sau:

```bash
import sys

for line in sys.stdin:
line = line.strip()
if not line:
continue
fields = line.split()
if len(fields) < 4:
continue
time_field = fields[2] # Trường thứ 3 là thời gian (HH:MM:SS)
hour = time_field.split(":")[0]
print(f"{hour}\t1")
```

Lưu file.

1.4 Tạo file reducer_hour.py
Mở terminal:

```bash
nano reducer_hour.py
```

Dán nội dung sau:

```bash
import sys

current_hour = None
count = 0

for line in sys.stdin:
line = line.strip()
if not line:
continue
hour, value = line.split("\t")
value = int(value)

    if current_hour == hour:
        count += value
    else:
        if current_hour is not None:
            print(f"{current_hour}:00 - {current_hour}:59\t{count}")
        current_hour = hour
        count = value

if current_hour is not None:
print(f"{current_hour}:00 - {current_hour}:59\t{count}")
```

Lưu file.

1.5 Cấp quyền thực thi cho các file
Trong terminal, chạy lệnh:

```bash
chmod +x mapper_date.py reducer_date.py mapper_hour.py reducer_hour.py
```

2. Upload dữ liệu lên HDFS

   Khởi động hdoop

   ```bash
   su - hdoop
   ```

   ```bash
   start-all.sh
   ```

   Giả sử file ElonMusk_tweets.csv nằm tại /home/phat/Downloads/ElonMusk_tweets.csv. Upload file lên HDFS với tài khoản hdoop bằng lệnh:

```bash
hdfs dfs -mkdir -p /user/hdoop/data
hdfs dfs -copyFromLocal /home/phat/Downloads/ElonMusk_tweets.csv /user/hdoop/data
```

3. Chạy các job MapReduce

3.1 Đếm số tweet theo ngày
Chạy job MapReduce với lệnh:

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-\*.jar \
 -input /user/hdoop/data/ElonMusk_tweets.csv \
 -output /user/hdoop/data/tweet_count_by_date \
 -mapper mapper_date.py \
 -reducer reducer_date.py
```

Sau khi job hoàn tất, xem kết quả:

```bash
hdfs dfs -cat /user/hdoop/data/tweet_count_by_date/part-\*
```

3.2 Đếm số tweet theo khung giờ
Chạy job MapReduce với lệnh:

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-\*.jar \
 -input /user/hdoop/data/ElonMusk_tweets.csv \
 -output /user/hdoop/data/tweet_count_by_hour \
 -mapper mapper_hour.py \
 -reducer reducer_hour.py
```

Xem kết quả:

```bash
hdfs dfs -cat /user/hdoop/data/tweet_count_by_hour/part-\*
```

```bash
stop-all.sh
```

Phần II: Apache Spark và Trực Quan Hóa Dữ Liệu

1. Tạo file Spark Job với Biểu Đồ: tweet_analysis.py
   Mở terminal:

```bash
nano tweet_analysis.py
```

Dán nội dung sau vào file (chỉnh sửa đường dẫn file nếu cần: nếu dữ liệu đang ở local hoặc trên HDFS):

```bash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, hour, to_timestamp, concat_ws, max as spark_max

# Tạo SparkSession
spark = SparkSession.builder.appName("ElonMuskTweetAnalysis").getOrCreate()

# Đọc file CSV với header từ đường dẫn
df = spark.read.csv(
    "file:///home/phat/Downloads/tweet/ElonMusk_tweets.csv",
    header=True,
    inferSchema=True
)

# Kiểm tra schema
df.printSchema()

# ----------------------------------------------------------
# (a) Đếm số tweets theo ngày
# ----------------------------------------------------------
tweets_by_date = df.groupBy("date").agg(count("*").alias("tweet_count"))
tweets_by_date_sorted = tweets_by_date.orderBy(col("date"))

print("=== Số lượng tweet theo ngày ===")
tweets_by_date_sorted.show()

# Lưu kết quả ra CSV (gộp thành 1 file duy nhất)
tweets_by_date_sorted.coalesce(1).write.csv("tweet_count_by_date_single", header=True)

# ----------------------------------------------------------
# (b) Đếm số tweets theo khung giờ
# ----------------------------------------------------------
# Tạo cột timestamp từ 'date' và 'time'
df_with_ts = df.withColumn(
    "timestamp",
    to_timestamp(concat_ws(" ", col("date"), col("time")), "yyyy-MM-dd HH:mm:ss")
)

# Tạo cột 'hour' từ timestamp
df_with_hour = df_with_ts.withColumn("hour", hour(col("timestamp")))

# Đếm tweet theo 'hour'
tweets_by_hour = df_with_hour.groupBy("hour").agg(count("*").alias("tweet_count"))
tweets_by_hour_sorted = tweets_by_hour.orderBy(col("hour"))

print("=== Số lượng tweet theo từng khung giờ (0-23) ===")
tweets_by_hour_sorted.show()

# Tìm giá trị tweet_count lớn nhất để đánh dấu khung giờ tweet nhiều nhất
max_count_val = tweets_by_hour_sorted.agg(spark_max("tweet_count")).first()[0]

# Thêm cột 'most_active': True nếu tweet_count == max_count_val, ngược lại False
tweets_by_hour_flagged = tweets_by_hour_sorted.withColumn("most_active", col("tweet_count") == max_count_val)

# Lưu kết quả ra CSV (gộp thành 1 file duy nhất)
tweets_by_hour_flagged.coalesce(1).write.csv("tweet_count_by_hour_single", header=True)

# In ra khung giờ có số tweet nhiều nhất
most_active = tweets_by_hour_flagged.filter(col("most_active") == True).collect()
if most_active:
    most_active = most_active[0]
    print(f"Elon Musk thường đăng nhiều tweet nhất vào khung giờ {most_active['hour']}h "
          f"với {most_active['tweet_count']} tweet.")
else:
    print("Không tìm thấy khung giờ có tweet nhiều nhất.")

# Dừng SparkSession
spark.stop()


```

Lưu file (Ctrl+X, Y, Enter).

2. Chạy Spark Job

Trong terminal, chạy lệnh:

```bash
spark-submit tweet_analysis.py
```

Kết quả sẽ được trực quan hóa bằng biểu đồ và các file ảnh tweet_count_by_date.png và tweet_count_by_hour.png sẽ được lưu trong thư mục làm việc.
