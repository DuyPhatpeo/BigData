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
   Giả sử file ElonMusk_tweets.csv nằm tại /home/phat/Downloads/ElonMusk_tweets.csv. Upload file lên HDFS với tài khoản hdoop bằng lệnh:

bash
Sao chép
Chỉnh sửa
hdfs dfs -mkdir -p /user/hdoop/data
hdfs dfs -copyFromLocal /home/phat/Downloads/ElonMusk_tweets.csv /user/hdoop/data 3. Chạy các job MapReduce
3.1 Đếm số tweet theo ngày
Chạy job MapReduce với lệnh:

bash
Sao chép
Chỉnh sửa
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-\*.jar \
 -input /user/hdoop/data/ElonMusk_tweets.csv \
 -output /user/hdoop/data/tweet_count_by_date \
 -mapper mapper_date.py \
 -reducer reducer_date.py
Sau khi job hoàn tất, xem kết quả:

bash
Sao chép
Chỉnh sửa
hdfs dfs -cat /user/hdoop/data/tweet_count_by_date/part-\*
3.2 Đếm số tweet theo khung giờ
Chạy job MapReduce với lệnh:

bash
Sao chép
Chỉnh sửa
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-\*.jar \
 -input /user/hdoop/data/ElonMusk_tweets.csv \
 -output /user/hdoop/data/tweet_count_by_hour \
 -mapper mapper_hour.py \
 -reducer reducer_hour.py
Xem kết quả:

bash
Sao chép
Chỉnh sửa
hdfs dfs -cat /user/hdoop/data/tweet_count_by_hour/part-\*
Phần II: Apache Spark và Trực Quan Hóa Dữ Liệu

1. Tạo file Spark Job với Biểu Đồ: tweet_analysis.py
   Mở terminal:

bash
Sao chép
Chỉnh sửa
nano tweet_analysis.py
Dán nội dung sau vào file (chỉnh sửa đường dẫn file nếu cần: nếu dữ liệu đang ở local hoặc trên HDFS):

python
Sao chép
Chỉnh sửa
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd

# Khởi tạo SparkSession

spark = SparkSession.builder.appName("TweetAnalysis").getOrCreate()
sc = spark.sparkContext

# Đọc file dữ liệu.

# Nếu dữ liệu ở local:

rdd = sc.textFile("file:///home/phat/Downloads/ElonMusk_tweets.csv")

# Nếu dữ liệu đã upload lên HDFS, dùng:

# rdd = sc.textFile("hdfs://localhost:9000/user/hdoop/data/ElonMusk_tweets.csv")

# (a) Đếm số tweet theo ngày

def extract_date(line):
fields = line.split()
if len(fields) < 4:
return None
return (fields[1], 1) # (date, 1)

tweet_by_date = rdd.map(extract_date) \
 .filter(lambda x: x is not None) \
 .reduceByKey(lambda a, b: a + b)

# Thu thập kết quả và chuyển sang DataFrame

data_date = tweet_by_date.collect()
df_date = pd.DataFrame(data_date, columns=['Date', 'Count'])
df_date.sort_values('Date', inplace=True)

# Vẽ biểu đồ số tweet theo ngày

plt.figure(figsize=(10, 5))
plt.bar(df_date['Date'], df_date['Count'], color='skyblue')
plt.title('Số Tweet theo Ngày')
plt.xlabel('Ngày')
plt.ylabel('Số Tweet')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("tweet_count_by_date.png") # Lưu biểu đồ dưới dạng file ảnh
plt.show()

# (b) Đếm số tweet theo khung giờ

def extract_hour(line):
fields = line.split()
if len(fields) < 4:
return None
hour = fields[2].split(":")[0]
return (hour, 1)

tweet_by_hour = rdd.map(extract_hour) \
 .filter(lambda x: x is not None) \
 .reduceByKey(lambda a, b: a + b)
data_hour = tweet_by_hour.collect()
df_hour = pd.DataFrame(data_hour, columns=['Hour', 'Count'])
df_hour.sort_values('Hour', inplace=True)

# Vẽ biểu đồ số tweet theo khung giờ

plt.figure(figsize=(8, 5))
plt.bar(df_hour['Hour'], df_hour['Count'], color='salmon')
plt.title('Số Tweet theo Khung giờ')
plt.xlabel('Giờ')
plt.ylabel('Số Tweet')
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig("tweet_count_by_hour.png") # Lưu biểu đồ
plt.show()

spark.stop()
Lưu file (Ctrl+X, Y, Enter).

2. Cài đặt thư viện (nếu chưa có)
   Cài đặt các thư viện cần thiết bằng pip:

bash
Sao chép
Chỉnh sửa
pip3 install matplotlib pandas 3. Chạy Spark Job
Trong terminal, chạy lệnh:

bash
Sao chép
Chỉnh sửa
spark-submit tweet_analysis.py
Kết quả sẽ được trực quan hóa bằng biểu đồ và các file ảnh tweet_count_by_date.png và tweet_count_by_hour.png sẽ được lưu trong thư mục làm việc.

```

```
