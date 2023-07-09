import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType


IN_TOPIC_NAME = 'student.topic.cohort11.frustamov'
OUT_TOPIC_NAME = 'student.topic.cohort11.frustamov.out'

# kafka creds
user = 'de-student'
password = 'ltcneltyn'

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df_feedback = df.withColumn('feedback', F.lit(None).cast(StringType()))
    #df_feedback.show()

    df_feedback.write.format('jdbc').mode('append')\
        .option('url', 'jdbc:postgresql://localhost:5432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'public.subscribers_feedback') \
        .option('user', 'jovyan') \
        .option('password', 'jovyan') \
        .save()

    # создаём df для отправки в Kafka. Сериализация в json.
    df_serialized = df_feedback.drop("feedback").select(F.to_json(F.struct(F.col('*'))).alias('value')).select('value')
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df_serialized.write \
        .format('kafka') \
        .mode("append")\
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{user}" password="{password}";') \
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
        .option('topic', OUT_TOPIC_NAME) \
        .option("truncate", False) \
        .save()
    # очищаем память от df
    df.unpersist()

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{user}" password="{password}";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', IN_TOPIC_NAME) \
    .load()

# restaurant_read_stream_df.printSchema()

# определяем схему входного сообщения для json
incoming_message_schema = StructType([
    StructField('restaurant_id', StringType(), nullable=True),
    StructField('adv_campaign_id', StringType(), nullable=True),
    StructField('adv_campaign_content', StringType(), nullable=True),
    StructField('adv_campaign_owner', StringType(), nullable=True),
    StructField('adv_campaign_owner_contact', StringType(), nullable=True),
    StructField('adv_campaign_datetime_start', LongType(), nullable=True),
    StructField('adv_campaign_datetime_end', LongType(), nullable=True),
    StructField('datetime_created', LongType(), nullable=True),
])


# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (
    restaurant_read_stream_df.withColumn("value", F.col("value").cast(StringType()))
    .withColumn("event", F.from_json(F.col("value"), incoming_message_schema))
    .selectExpr("event.*")
    # возможно datetime_created нужно использовать
    .withColumn('dt', F.from_unixtime(F.col('adv_campaign_datetime_end'), "yyyy-MM-dd' 'HH:mm:ss.SSS")
                .cast(TimestampType()))
    .withWatermark("dt", "1 minutes").drop("dt")
    .where(
        f"adv_campaign_datetime_start <= {current_timestamp_utc} and adv_campaign_datetime_end > {current_timestamp_utc}"
    )
)
# filtered_read_stream_df.writeStream.outputMode("append").format("console").start().awaitTermination() 
    
# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'public.subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load() \
                    .select('restaurant_id', 'client_id') \
                    .distinct()

subscribers_restaurant_df.show(truncate=False, n=15)

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
# print(current_timestamp_utc)
result_df = filtered_read_stream_df\
    .join(subscribers_restaurant_df, on="restaurant_id", how="inner")\
    .withColumn("trigger_datetime_created", F.lit(f"{current_timestamp_utc}").cast(LongType()))

# result_df.writeStream.outputMode("append").format("console").start().awaitTermination() 

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()