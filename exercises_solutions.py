"""
Rozwiązania ćwiczeń PySpark - SQL to PySpark Guide
Ćwiczenia do samodzielnego rozwiązania i sprawdzenia
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

class PySparkExercises:
    def __init__(self):
        # Windows-compatible configuration
        self.spark = SparkSession.builder \
            .appName("PySparkExercises") \
            .master("local[1]") \
            .config("spark.driver.memory", "512m") \
            .config("spark.python.worker.reuse", "false") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        self.create_sample_data()
    
    def create_sample_data(self):
        """Tworzy te same dane co w przewodniku."""
        
        # Tabela employees
        self.spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW employees AS
            SELECT * FROM VALUES 
                (1, 'Jan Kowalski', 'IT', 5000, '2020-01-15'),
                (2, 'Anna Nowak', 'HR', 4500, '2019-03-20'),
                (3, 'Piotr Wiśniewski', 'IT', 6000, '2021-06-10'),
                (4, 'Maria Kowalczyk', 'Finance', 5500, '2020-11-05'),
                (5, 'Tomasz Zieliński', 'IT', 4800, '2022-02-28')
            AS t(id, name, department, salary, hire_date)
        """)
        
        self.employees_df = self.spark.table("employees")
    
    def exercise_1(self):
        """1. Znajdź pracowników zatrudnionych w 2020 roku"""
        print("=== ĆWICZENIE 1: Pracownicy zatrudnieni w 2020 roku ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: użyj year(col('hire_date')) == 2020
        
        print("Twoje rozwiązanie SQL:")
        # sql_result = self.spark.sql("SELECT ...")
        # sql_result.show()
        
        print("\nTwoje rozwiązanie DataFrame API:")
        # df_result = self.employees_df.filter(...)
        # df_result.show()
        
        pass
    
    def exercise_2(self):
        """2. Oblicz medianę pensji w każdym dziale"""
        print("\n=== ĆWICZENIE 2: Mediana pensji w każdym dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: użyj percentile_approx(col('salary'), 0.5)
        
        pass
    
    def exercise_3(self):
        """3. Znajdź działy z najwyższą średnią pensją"""
        print("\n=== ĆWICZENIE 3: Działy z najwyższą średnią pensją ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: groupBy + agg + orderBy + limit(1)
        
        pass
    
    def exercise_4(self):
        """4. Stwórz ranking pracowników według pensji w każdym dziale"""
        print("\n=== ĆWICZENIE 4: Ranking pracowników w dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: Window.partitionBy('department').orderBy(col('salary').desc())
        
        pass
    
    def exercise_5(self):
        """5. Znajdź pracowników z pensją wyższą niż średnia w ich dziale"""
        print("\n=== ĆWICZENIE 5: Pensja wyższa niż średnia w dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: Window function z avg() OVER (PARTITION BY department)
        
        pass
    
    def run_exercises(self):
        """Uruchom wszystkie ćwiczenia."""
        self.exercise_1()
        self.exercise_2()
        self.exercise_3()
        self.exercise_4()
        self.exercise_5()
        
        self.spark.stop()

if __name__ == "__main__":
    exercises = PySparkExercises()
    exercises.run_exercises()