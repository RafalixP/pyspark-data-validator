"""
Prosty test PySpark - sprawdzenie czy działa
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def test_pyspark():
    # Konfiguracja Spark dla Windows - naprawka dla Python worker crash
    spark = SparkSession.builder \
        .appName("SimpleTest") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.sql.adaptive.skewJoin.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.python.worker.reuse", "false") \
        .getOrCreate()
    
    # Ustawienie poziomu logowania
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Proste dane testowe
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        
        # Tworzenie DataFrame
        df = spark.createDataFrame(data, columns)
        
        print("✅ PySpark działa!")
        print("DataFrame utworzony pomyślnie:")
        
        # Podstawowe operacje - bez count() który może crashować
        print("Schemat:")
        df.printSchema()
        
        # Collect danych (bezpieczniejsze niż count)
        print("Dane:")
        rows = df.collect()
        for row in rows:
            print(f"  {row.name}: {row.age} lat")
        
        print(f"Liczba wierszy: {len(rows)}")
        
        # Filtrowanie - tylko collect, bez count
        filtered = df.filter(col("age") > 25)
        filtered_rows = filtered.collect()
        print(f"Wierszy po filtrowaniu (age > 25): {len(filtered_rows)}")
        for row in filtered_rows:
            print(f"  Filtrowane: {row.name}: {row.age} lat")
            
        print("\n🎉 Test zakończony pomyślnie!")
        
    except Exception as e:
        print(f"❌ Błąd: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_pyspark()