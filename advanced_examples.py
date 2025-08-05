"""
Zaawansowane przykłady PySpark dla Data Engineering
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


class AdvancedPySparkExamples:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("AdvancedDataEngineering") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def data_quality_pipeline(self, df):
        """Kompletny pipeline walidacji jakości danych."""
        
        # 1. Dodanie timestamp przetwarzania
        df_with_timestamp = df.withColumn("processed_at", current_timestamp())
        
        # 2. Flagowanie rekordów z problemami jakościowymi
        quality_df = df_with_timestamp \
            .withColumn("has_nulls", 
                       when(col("name").isNull() | col("email").isNull(), True).otherwise(False)) \
            .withColumn("email_valid",
                       when(col("email").rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'), True).otherwise(False)) \
            .withColumn("age_valid",
                       when((col("age") >= 0) & (col("age") <= 120), True).otherwise(False))
        
        # 3. Obliczenie quality score
        quality_df = quality_df.withColumn(
            "quality_score",
            (when(col("has_nulls") == False, 1).otherwise(0) +
             when(col("email_valid") == True, 1).otherwise(0) +
             when(col("age_valid") == True, 1).otherwise(0)) / 3.0
        )
        
        return quality_df
    
    def detect_anomalies(self, df, column_name):
        """Wykrywanie anomalii używając IQR."""
        
        # Obliczenie kwartyli
        quantiles = df.approxQuantile(column_name, [0.25, 0.75], 0.05)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        
        # Granice dla outlierów
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        # Flagowanie anomalii
        anomaly_df = df.withColumn(
            f"{column_name}_anomaly",
            when((col(column_name) < lower_bound) | (col(column_name) > upper_bound), True).otherwise(False)
        )
        
        return anomaly_df, {"q1": q1, "q3": q3, "iqr": iqr, "bounds": (lower_bound, upper_bound)}
    
    def data_lineage_tracking(self, df):
        """Śledzenie pochodzenia danych."""
        
        # Dodanie metadanych o źródle i transformacjach
        lineage_df = df \
            .withColumn("source_system", lit("example_system")) \
            .withColumn("ingestion_time", current_timestamp()) \
            .withColumn("transformation_version", lit("v1.0")) \
            .withColumn("record_hash", sha2(concat_ws("|", *df.columns), 256))
        
        return lineage_df
    
    def sliding_window_analysis(self, df, timestamp_col, value_col):
        """Analiza w oknie przesuwnym - typowa dla monitoringu jakości danych."""
        
        # Definicja okna czasowego
        window_spec = Window.partitionBy().orderBy(col(timestamp_col)).rangeBetween(-86400, 0)  # 24h window
        
        # Obliczenia w oknie
        windowed_df = df \
            .withColumn("avg_24h", avg(col(value_col)).over(window_spec)) \
            .withColumn("count_24h", count(col(value_col)).over(window_spec)) \
            .withColumn("stddev_24h", stddev(col(value_col)).over(window_spec))
        
        return windowed_df
    
    def data_sampling_strategies(self, df):
        """Różne strategie próbkowania danych."""
        
        strategies = {}
        
        # 1. Próbkowanie losowe
        strategies['random_10pct'] = df.sample(fraction=0.1, seed=42)
        
        # 2. Próbkowanie stratyfikowane
        strategies['stratified'] = df.sampleBy("age", {25: 0.2, 30: 0.3, 35: 0.1}, seed=42)
        
        # 3. Próbkowanie systematyczne (co n-ty rekord)
        strategies['systematic'] = df.withColumn("row_num", monotonically_increasing_id()) \
                                    .filter(col("row_num") % 10 == 0)
        
        return strategies
    
    def performance_monitoring(self, df):
        """Monitorowanie wydajności przetwarzania."""
        
        # Partycjonowanie dla lepszej wydajności
        partitioned_df = df.repartition(4, col("age"))
        
        # Cache dla często używanych DataFrame
        cached_df = partitioned_df.cache()
        
        # Statystyki wykonania
        execution_stats = {
            'partition_count': cached_df.rdd.getNumPartitions(),
            'record_count': cached_df.count(),
            'is_cached': cached_df.is_cached
        }
        
        return cached_df, execution_stats
    
    def close(self):
        """Zamknięcie sesji Spark."""
        self.spark.stop()


def demo_advanced_features():
    """Demonstracja zaawansowanych funkcji."""
    
    advanced = AdvancedPySparkExamples()
    spark = advanced.spark
    
    # Przykładowe dane z timestampami
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True),
        StructField("created_at", TimestampType(), True)
    ])
    
    from datetime import datetime, timedelta
    base_time = datetime.now()
    
    data = [
        (1, "Jan Kowalski", "jan@example.com", 30, 5000.0, base_time),
        (2, "Anna Nowak", "anna@test.pl", 25, 4500.0, base_time - timedelta(hours=1)),
        (3, "Piotr Wiśniewski", "invalid-email", 150, 50000.0, base_time - timedelta(hours=2)),  # anomalie
        (4, None, "test@example.com", -5, 1000.0, base_time - timedelta(hours=3)),
        (5, "Maria Kowalczyk", "maria@company.com", 28, 5500.0, base_time - timedelta(hours=4))
    ]
    
    df = spark.createDataFrame(data, schema)
    
    print("=== PIPELINE JAKOŚCI DANYCH ===")
    quality_df = advanced.data_quality_pipeline(df)
    quality_df.select("name", "quality_score", "has_nulls", "email_valid", "age_valid").show()
    
    print("\n=== WYKRYWANIE ANOMALII ===")
    anomaly_df, stats = advanced.detect_anomalies(df, "salary")
    print(f"Statystyki: {stats}")
    anomaly_df.select("name", "salary", "salary_anomaly").show()
    
    print("\n=== ŚLEDZENIE POCHODZENIA DANYCH ===")
    lineage_df = advanced.data_lineage_tracking(df)
    lineage_df.select("name", "source_system", "transformation_version").show(truncate=False)
    
    advanced.close()


if __name__ == "__main__":
    demo_advanced_features()