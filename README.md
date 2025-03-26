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
#!/usr/bin/env python3
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
#!/usr/bin/env python3
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
#!/usr/bin/env python3
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
#!/usr/bin/env python3
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

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("TweetAnalysisSimple").getOrCreate()
sc = spark.sparkContext

# Đường dẫn file dữ liệu (local)
file_path = "file:///home/phat/Downloads/tweet/ElonMusk_tweets.csv"

# Đọc file dữ liệu
rdd = sc.textFile(file_path)

# -------------------------------
# (a) Đếm số tweets của từng ngày
# -------------------------------
def extract_date(line):
    fields = line.split()
    # Kiểm tra dữ liệu có ít nhất 4 trường: id, date, time, text
    if len(fields) < 4:
        return None
    return (fields[1], 1)  # (date, 1)

tweet_by_date = (
    rdd.map(extract_date)
       .filter(lambda x: x is not None)
       .reduceByKey(lambda a, b: a + b)
)

results_date = tweet_by_date.collect()

# Lưu kết quả đếm theo ngày ra file txt
with open("tweet_count_by_date.txt", "w", encoding="utf-8") as f:
    f.write("Đếm số tweets của từng ngày:\n")
    for date, count in sorted(results_date):
        f.write(f"{date}\t{count}\n")

# -------------------------------
# (b) Đếm số tweets theo từng khung giờ
# -------------------------------
def extract_hour(line):
    fields = line.split()
    if len(fields) < 4:
        return None
    # Trường thứ 3 là thời gian (HH:MM:SS)
    hour = fields[2].split(":")[0]
    return (hour, 1)

tweet_by_hour = (
    rdd.map(extract_hour)
       .filter(lambda x: x is not None)
       .reduceByKey(lambda a, b: a + b)
)

results_hour = tweet_by_hour.collect()

# Lưu kết quả đếm theo khung giờ ra file txt
with open("tweet_count_by_hour.txt", "w", encoding="utf-8") as f:
    f.write("Đếm số tweets theo từng khung giờ:\n")
    for hour, count in sorted(results_hour, key=lambda x: int(x[0])):
        f.write(f"{int(hour):02d}:00\t{count}\n")

# ------------------------------------------
# Tìm khung giờ có số tweet nhiều nhất
# ------------------------------------------
if results_hour:
    max_hour, max_count = max(results_hour, key=lambda x: x[1])
    # Định dạng: hh:mm - hh:mm, ví dụ: 08:00 - 08:59
    max_hour_int = int(max_hour)
    start_time = f"{max_hour_int:02d}:00"
    end_time = f"{max_hour_int:02d}:59"
    answer = (f"Elon Musk thường đăng tweet vào khung giờ: {start_time} - {end_time} "
              f"(với {max_count} tweet).")
else:
    answer = "Không tìm thấy dữ liệu tweet theo giờ."

# Lưu kết quả trả lời ra file answer.txt
with open("tweet_hour_answer.txt", "w", encoding="utf-8") as f:
    f.write(answer + "\n")

# Dừng Spark
spark.stop()

# In kết quả ra console
print("Đã lưu kết quả vào các file:")
print("  - tweet_count_by_date.txt")
print("  - tweet_count_by_hour.txt")
print("  - tweet_hour_answer.txt")

```

Lưu file (Ctrl+X, Y, Enter).

2. Cài đặt thư viện (nếu chưa có)
   Cài đặt các thư viện cần thiết bằng pip:

```bash
pip3 install matplotlib pandas
```

3. Chạy Spark Job

Trong terminal, chạy lệnh:

```bash
spark-submit tweet_analysis.py
```

Kết quả sẽ được trực quan hóa bằng biểu đồ và các file ảnh tweet_count_by_date.png và tweet_count_by_hour.png sẽ được lưu trong thư mục làm việc.
