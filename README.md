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

0. Đăng nhập tài khoản hdoop
   Trước khi tạo file và thực hiện các bước khác, hãy đăng nhập vào tài khoản hdoop. Mở terminal và chạy lệnh:

```bash
su - hdoop
```

1. Tạo các file Python cho MapReduce
   Sau khi đã đăng nhập tài khoản hdoop, bạn tiến hành tạo các file như sau:

1.1. Tạo file mapper_date.py
Mở terminal và nhập lệnh:

```bash
nano mapper_date.py
```

Dán nội dung sau:

```bash
#!/usr/bin/python3
import sys
import csv

for line in sys.stdin:
    try:
        row = next(csv.reader([line]))  # Đọc dòng CSV
        tweet_id, created_at, text = row
        date = created_at[:10]  # Lấy năm từ created_at (YYYY-MM-DD HH:MM:SS)
        print(f"{date}\t1")  # Output: (date, 1)
    except Exception as e:
        continue  # Bỏ qua dòng lỗi
```

Lưu file bằng cách nhấn Ctrl+X, sau đó nhấn Y rồi Enter.

1.2. Tạo file reducer_date.py

```bash

nano reducer_date.py
```

Dán nội dung sau:

```bash
#!usr/bin/python3
import sys
from collections import defaultdict

tweet_count = defaultdict(int)

for line in sys.stdin:
    day, count = line.strip().split("\t")
    tweet_count[day] += int(count)

for day in sorted(tweet_count):
    print(f"{day}\t{tweet_count[day]}")
```

Lưu file.

1.3. Tạo file mapper_hour.py
Mở terminal:

```bash
nano mapper_hour.py
```

Dán nội dung sau (chú ý indent đúng):

```bash
#!/usr/bin/python3
import sys
import csv

for line in sys.stdin:
    try:
        row = next(csv.reader([line]))  # Đọc dòng CSV
        tweet_id, created_at, text = row
        hour = created_at[11:13]  # Lấy giờ từ created_at (YYYY-MM-DD HH:MM:SS)
        print(f"{hour}\t1")  # Output: (hour, 1)
    except Exception:
        continue  # Bỏ qua dòng lỗi

```

Lưu file.

1.4. Tạo file reducer_hour.py
Mở terminal:

```bash
nano reducer_hour.py
```

Dán nội dung sau (chú ý indent đúng):

```bash
#!/usr/bin/python3
import sys
from collections import defaultdict

tweet_count = defaultdict(int)

for line in sys.stdin:
    hour, count = line.strip().split("\t")
    tweet_count[hour] += int(count)

for hour in sorted(tweet_count):
    print(f"{hour}\t{tweet_count[hour]}")

```

Lưu file.

1.5. Cấp quyền thực thi cho các file
Chạy lệnh sau để cấp quyền thực thi:

```bash
chmod +x mapper_date.py reducer_date.py mapper_hour.py reducer_hour.py
```

2. Upload dữ liệu lên HDFS
   Chạy Hadoop
   Sau khi hoàn tất công việc, dừng Hadoop bằng lệnh:

```bash
start-all.sh
```

Giả sử file dữ liệu ElonMusk_tweets.csv đã được lưu tại /mnt/data/ElonMusk_tweets.csv. Từ tài khoản hdoop, upload file lên HDFS bằng các lệnh sau:

```bash
hdfs dfs -mkdir -p data
```

```bash
hdfs dfs -copyFromLocal /home/phat/Downloads/tweet /user/hdoop/data
```

1. Chạy các job MapReduce

Bạn có thể kiểm tra xem file ElonMusk_tweets.csv đã được chuyển vào HDFS chưa bằng lệnh:

```bash
hdfs dfs -ls /user/hdoop/data
```

xoa file

```
hdfs dfs -rm -r /user/hdoop/data/tweet_count_by_hour
```

3.1. Đếm số tweet theo ngày
Chạy job MapReduce với lệnh:

```
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
  -file mapper_date.py -mapper mapper_date.py \
  -file reducer_date.py -reducer reducer_date.py \
  -input /user/hdoop/data/tweet \
  -output /user/hdoop/data/tweet_count_by_date

```

Sau khi job hoàn tất, xem kết quả bằng lệnh:

```bash
hdfs dfs -cat /user/hdoop/data/tweet_count_by_date/part-\*
```

3.2. Đếm số tweet theo khung giờ
Chạy job MapReduce với lệnh:

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
  -file mapper_hour.py -mapper mapper_hour.py \
  -file reducer_hour.py -reducer reducer_hour.py \
  -input /user/hdoop/data/tweet \
  -output /user/hdoop/data/tweet_count_by_hour
```

Xem kết quả bằng lệnh:

```bash
hdfs dfs -cat /user/hdoop/data/tweet_count_by_hour/part-\*
```

4. Dừng Hadoop
   Sau khi hoàn tất công việc, dừng Hadoop bằng lệnh:

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
spark = SparkSession.builder.appName("TweetAnalysis").getOrCreate()
sc = spark.sparkContext

# Đọc file dữ liệu
rdd = sc.textFile("file:///mnt/data/ElonMusk_tweets.csv")

# Bỏ dòng tiêu đề
header = rdd.first()
rdd = rdd.filter(lambda line: line != header)

# (a) Đếm số tweet theo ngày
def extract_date(line):
    fields = line.split(",")
    if len(fields) < 2:
        return None
    date = fields[1].split(" ")[0]  # Lấy phần YYYY-MM-DD
    return (date, 1)

tweet_by_date = rdd.map(extract_date).filter(lambda x: x is not None).reduceByKey(lambda a, b: a + b)
tweet_by_date_sorted = tweet_by_date.sortByKey()
tweet_by_date_sorted.coalesce(1).saveAsTextFile("tweet_count_by_date")

# In ra 10 dòng đầu tiên
tweet_by_date_sorted.take(5)

# (b) Đếm số tweet theo khung giờ
def extract_hour(line):
    fields = line.split(",")
    if len(fields) < 2:
        return None
    hour = fields[1].split(" ")[1].split(":")[0]  # Lấy giờ (HH)
    return (hour, 1)

tweet_by_hour = rdd.map(extract_hour).filter(lambda x: x is not None).reduceByKey(lambda a, b: a + b)
tweet_by_hour_sorted = tweet_by_hour.sortByKey()
tweet_by_hour_sorted.coalesce(1).saveAsTextFile("tweet_count_by_hour")

# In ra 10 dòng đầu tiên
tweet_by_hour_sorted.take(5)

# (c) Tìm khung giờ Elon Musk hay đăng tweet nhất
most_active_hour = tweet_by_hour.max(lambda x: x[1])
print(f"Khung giờ Elon Musk hay đăng tweet nhất: {most_active_hour[0]}h với {most_active_hour[1]} tweet")

# Dừng SparkSession
spark.stop()



```

Lưu file (Ctrl+X, Y, Enter).

2. Chạy Spark Job

Trong terminal, chạy lệnh:

```bash
spark-submit tweet_analysis.py
```

Cai Spark

1. Tải và giải nén Spark
   Tải phiên bản Spark 3.5.5:

```bash
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
```

Giải nén:

```bash
tar xvf spark-3.5.5-bin-hadoop3.tgz
```

Di chuyển thư mục Spark:

```bash
sudo mv spark-3.5.5-bin-hadoop3 /opt/spark
```

2. Thiết lập biến môi trường
   Mở file ~/.bashrc và thêm các dòng sau:

```
echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
echo "export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH" >> ~/.bashrc
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
```

Nạp lại cấu hình:

```bash
source ~/.bashrc
```

3. Khởi động PySpark

```
pyspark
```

```

```
