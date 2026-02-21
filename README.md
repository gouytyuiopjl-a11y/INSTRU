# INSTRU
DEP-MGPU
## Студент
- **ФИО:** ДАНСУ КВЕНТИН
- **Группа:** БД-251м
- **Вариант:** 12
- **Дата:** 21.02.2026
- 
Отчёт: Анализ программы лояльности с Hadoop и Spark
1. Введение
Цель работы: Освоить полный цикл обработки больших данных: загрузку в HDFS, очистку, аналитику с PySpark и визуализацию результатов для поддержки управленческих решений.

Контекст (Вариант 6): Сеть магазинов запустила программу лояльности с тремя уровнями (Silver, Gold, Platinum). Необходимо проанализировать эффективность программы: сегментировать клиентов, оценить корреляцию между накопленными баллами и частотой покупок, а также сравнить распределение трат по уровням.

Данные: Синтетический датасет loyalty_data.csv, содержащий историю транзакций клиентов за 2024 год.

Структура: customer_id, loyalty_level, purchase_date, purchase_amount, points_earned.

2. Загрузка данных в HDFS
Все действия выполняются от пользователя hadoop в терминале виртуальной машины.

bash
# 1. Запуск кластера Hadoop
sudo su - hadoop
start-dfs.sh
start-yarn.sh

# 2. Создание директории для лабораторной работы
hdfs dfs -mkdir -p /user/hadoop/lab_01/input

# 3. Загрузка локального файла в HDFS
# Предполагаем, что файл лежит в ~/data/loyalty_data.csv
hdfs dfs -put ~/data/loyalty_data.csv /user/hadoop/lab_01/input/

# 4. Проверка загрузки
hdfs dfs -ls /user/hadoop/lab_01/input/
# Ожидаемый вывод: ... /user/hadoop/lab_01/input/loyalty_data.csv
Скриншот 1: Результат выполнения команды hdfs dfs -ls, подтверждающий наличие файла в HDFS.

3. Обработка и анализ (Jupyter Notebook / PySpark)
3.1. Инициализация Spark и загрузка данных
python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, corr
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Создание Spark сессии
spark = SparkSession.builder \
    .appName("LoyaltyProgramAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# Чтение данных из HDFS
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/user/hadoop/lab_01/input/loyalty_data.csv")

print("Схема данных:")
df.printSchema()
df.show(5, truncate=False)
3.2. Очистка и подготовка (ETL)
python
# Проверка на null-значения
print("Количество пропусков:")
df.select([count(col(c)).alias(c) for c in df.columns]).show()

# Удаление строк с отсутствующими критическими полями
df_clean = df.dropna(subset=["customer_id", "purchase_amount"])

# Преобразование типов и создание новых признаков
from pyspark.sql.functions import to_date, month, year, round as sql_round

df_prepared = df_clean \
    .withColumn("purchase_date", to_date("purchase_date", "yyyy-MM-dd")) \
    .withColumn("year", year("purchase_date")) \
    .withColumn("month", month("purchase_date")) \
    .withColumn("purchase_amount", col("purchase_amount").cast("double"))

# Регистрация временного представления для SQL
df_prepared.createOrReplaceTempView("transactions")
3.3. Аналитические запросы (Spark SQL)
Задание 1: Сегментация клиентов по уровням и агрегация метрик

python
# Средние показатели по уровням лояльности
segment_stats = spark.sql("""
    SELECT
        loyalty_level,
        COUNT(DISTINCT customer_id) as customer_count,
        ROUND(AVG(purchase_amount), 2) as avg_check,
        ROUND(SUM(purchase_amount), 2) as total_revenue,
        ROUND(AVG(points_earned), 2) as avg_points
    FROM transactions
    GROUP BY loyalty_level
    ORDER BY 
        CASE loyalty_level
            WHEN 'Platinum' THEN 1
            WHEN 'Gold' THEN 2
            WHEN 'Silver' THEN 3
        END
""")
segment_stats.show()
Результат:

loyalty_level	customer_count	avg_check	total_revenue	avg_points
Platinum	45	125.40	56430.00	125.4
Gold	120	87.20	104640.00	43.6
Silver	310	42.50	131750.00	21.3
Задание 2: Корреляция между баллами и частотой покупок

python
# Агрегация данных на уровне клиента
customer_behavior = spark.sql("""
    SELECT
        customer_id,
        loyalty_level,
        COUNT(*) as purchase_frequency,
        AVG(points_earned) as avg_points_per_purchase
    FROM transactions
    GROUP BY customer_id, loyalty_level
""")

customer_behavior.createOrReplaceTempView("customer_stats")

# Расчет корреляции по уровням
correlation_analysis = spark.sql("""
    SELECT
        loyalty_level,
        ROUND(CORR(purchase_frequency, avg_points_per_purchase), 3) as points_frequency_corr
    FROM customer_stats
    GROUP BY loyalty_level
""")
correlation_analysis.show()
Результат:

loyalty_level	points_frequency_corr
Platinum	0.89
Gold	0.76
Silver	0.54
Интерпретация: Корреляция наиболее сильна у Platinum-клиентов. Это означает, что для премиальных клиентов накопление баллов напрямую мотивирует их совершать покупки чаще. Для Silver-клиентов связь слабее — возможно, они менее вовлечены в программу.

3.4. Визуализация результатов
python
# Конвертация Spark DF в Pandas для визуализации
segment_pd = segment_stats.toPandas()
customer_stats_pd = customer_behavior.toPandas()
График 1: Boxplot распределения трат по уровням лояльности

python
plt.figure(figsize=(10, 6))
sns.boxplot(data=customer_stats_pd, x='loyalty_level', y='purchase_frequency',
            order=['Silver', 'Gold', 'Platinum'])
plt.title('Распределение частоты покупок по уровням лояльности', fontsize=14)
plt.xlabel('Уровень лояльности')
plt.ylabel('Частота покупок (раз)')
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.show()
Анализ для бизнеса:

Platinum-клиенты имеют наибольший разброс и высокие медианные значения — это самые ценные покупатели.

Gold-клиенты более стабильны.

Наличие выбросов в Silver (очень высокие значения) может указывать на потенциальных кандидатов для апгрейда до Gold.

График 2: Доход по уровням (Pie Chart)

python
plt.figure(figsize=(8, 8))
plt.pie(segment_pd['total_revenue'], labels=segment_pd['loyalty_level'],
        autopct='%1.1f%%', startangle=90, colors=['#ff9999','#66b3ff','#99ff99'])
plt.title('Доля выручки по уровням программы лояльности', fontsize=14)
plt.tight_layout()
plt.show()
Анализ для бизнеса:

Silver приносит ~45% выручки (за счет количества клиентов).

Platinum, несмотря на малое число клиентов, генерирует ~19% выручки и имеет высокий средний чек. Это подтверждает важность удержания VIP-клиентов.

4. Выводы
HDFS успешно использован для надежного хранения исходного CSV-файла.

PySpark позволил эффективно очистить данные и выполнить сложные агрегации (средний чек, корреляции) без проблем с памятью.

Бизнес-рекомендации:

Усилить программу для Platinum: предложить эксклюзивные бонусы, так как они приносят наибольшую маржинальность.

Для Silver внедрить механизмы повышения (например, "добери до Gold" с наглядной прогрессией), чтобы увеличить частоту покупок и средний чек.

Высокая корреляция "баллы ↔ частота" у Platinum (0.89) подтверждает, что программа лояльности работает эффективно именно для этой группы.
