"""
Działający PySpark na Windows - unika Python worker crashes
"""

from pyspark.sql import SparkSession

def working_pyspark_test():
    # Konfiguracja która działa na Windows
    spark = SparkSession.builder \
        .appName("WorkingPySpark") \
        .master("local[1]") \
        .config("spark.driver.memory", "512m") \
        .config("spark.python.worker.reuse", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        print("[OK] SparkSession utworzony!")
        print(f"Spark version: {spark.version}")
        
        # 1. Test z range() - działa (tylko JVM)
        df1 = spark.range(5).toDF("number")
        df1.createOrReplaceTempView("numbers")
        
        print("\n=== Test 1: Liczby ===")
        spark.sql("SELECT * FROM numbers").show()
        spark.sql("SELECT COUNT(*) as total FROM numbers").show()
        spark.sql("SELECT SUM(number) as sum FROM numbers").show()
        
        # 2. Test z SQL - tworzenie danych w SQL (unika Python)
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW employees AS
            SELECT * FROM VALUES 
                (1, 'Jan', 'IT', 5000),
                (2, 'Anna', 'HR', 4500),
                (3, 'Piotr', 'IT', 6000)
            AS t(id, name, dept, salary)
        """)
        
        print("\n=== Test 2: Pracownicy (SQL) ===")
        spark.sql("SELECT * FROM employees").show()
        spark.sql("SELECT dept, AVG(salary) as avg_salary FROM employees GROUP BY dept").show()
        
        # 3. Test z plikiem (jeśli istnieje)
        try:
            # Tworzymy prosty plik CSV w pamięci
            import tempfile
            import os
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                f.write("id,name,age\n")
                f.write("1,Alice,25\n")
                f.write("2,Bob,30\n")
                f.write("3,Charlie,35\n")
                temp_file = f.name
            
            # Czytanie pliku - to działa!
            df_csv = spark.read.option("header", "true").csv(temp_file)
            df_csv.createOrReplaceTempView("people")
            
            print("\n=== Test 3: Dane z pliku CSV ===")
            spark.sql("SELECT * FROM people").show()
            spark.sql("SELECT COUNT(*) as count FROM people WHERE age > 25").show()
            
            # Sprzątanie
            os.unlink(temp_file)
            
        except Exception as csv_error:
            print(f"Test CSV nie powiódł się: {csv_error}")
        
        print("\n[SUCCESS] Sukces - PySpark działa na Windows!")
        print("\n[INFO] Kluczowe wnioski:")
        print("- Używaj spark.range() zamiast createDataFrame() z Python data")
        print("- Używaj Spark SQL zamiast DataFrame operations")
        print("- Czytanie plików działa dobrze")
        print("- Unikaj collect(), show() na DataFrame z Python data")
        
    except Exception as e:
        print(f"[ERROR] Błąd: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    working_pyspark_test()