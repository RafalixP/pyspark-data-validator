"""
Minimalny test PySpark - najbardziej podstawowa wersja
"""

from pyspark.sql import SparkSession

def minimal_test():
    # Działająca konfiguracja dla Windows
    spark = SparkSession.builder \
        .appName("MinimalTest") \
        .master("local[1]") \
        .config("spark.driver.memory", "512m") \
        .config("spark.python.worker.reuse", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        print("✅ SparkSession utworzony!")
        print(f"Spark version: {spark.version}")
        
        # Test z prostymi danymi - używamy Spark SQL zamiast Python operacji
        df = spark.range(3).toDF("id")
        df.createOrReplaceTempView("test_table")
        
        print("✅ DataFrame utworzony!")
        
        # Używamy Spark SQL - unika Python worker
        result = spark.sql("SELECT COUNT(*) as count FROM test_table")
        result.show()
        
        # Test z prostymi danymi tekstowymi
        data = [("Alice", 25), ("Bob", 30)]
        df2 = spark.createDataFrame(data, ["name", "age"])
        df2.createOrReplaceTempView("people")
        
        spark.sql("SELECT * FROM people WHERE age > 25").show()
        
        print("🎉 Sukces - PySpark działa z danymi!")
        
    except Exception as e:
        print(f"❌ Błąd: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    minimal_test()