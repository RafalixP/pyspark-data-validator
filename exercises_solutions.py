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
            .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
            .config("spark.local.dir", "C:/temp/spark-temp") \
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
    
    def exercise_0(self):
        """0. Wszyscy pracownicy"""
        print("=== Wszyscy pracownicy ===\n")
        
       
        
        print("Twoje rozwiązanie SQL:")
        sql_result = self.spark.sql("SELECT * FROM employees")
        sql_result.show()
        
        print("\nTwoje rozwiązanie DataFrame API:")
        df_result = self.employees_df.select('name', 'department', 'salary', 'hire_date')
        df_result.show()
        
        pass

    def exercise_1(self):
        """1. Znajdź pracowników zatrudnionych w 2020 roku"""
        print("=== ĆWICZENIE 1: Pracownicy zatrudnieni w 2020 roku ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: użyj year(col('hire_date')) == 2020
        
        print("Twoje rozwiązanie SQL:")
        sql_result = self.spark.sql("SELECT * FROM employees WHERE hire_date BETWEEN '2020-01-01' AND '2020-12-31'")
        sql_result.show()
        
        print("\nTwoje rozwiązanie DataFrame API:")
        df_result = self.employees_df.select('name', 'department', 'salary', 'hire_date').filter((col('hire_date') >= '2020-01-01') & (col('hire_date') <= '2020-12-31'))
        df_result.show()
        
        pass
    
    def exercise_2(self):
        """2. Oblicz medianę pensji w każdym dziale"""
        print("\n=== ĆWICZENIE 2: Mediana pensji w każdym dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: użyj percentile_approx(col('salary'), 0.5)
        
        print("\nTwoje rozwiązanie DataFrame API:")
        df_result = self.employees_df.groupBy('department').agg(percentile_approx(col('salary'), 0.5).alias('median_salary'))
        df_result.show()

        pass
    
    def exercise_3(self):
        """3. Znajdź działy z najwyższą średnią pensją"""
        print("\n=== ĆWICZENIE 3: Działy z najwyższą średnią pensją ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: groupBy + agg + orderBy + limit(1)
        
        print("\nTwoje rozwiązanie DataFrame API:")
        df_result = self.employees_df.groupBy('department').agg(round(avg('salary'),2).alias('avg_salary')).orderBy(col('avg_salary').desc())
        df_result.show()
        print('--- Naprawdę tylko najwyższa wartość ---')
        df_result = self.employees_df.groupBy('department').agg(avg('salary').alias('avg_salary')).orderBy(desc('avg_salary')).limit(1)
        df_result.show()
        pass
    
    def exercise_4(self):
        """4. Stwórz ranking pracowników według pensji w każdym dziale"""
        print("\n=== ĆWICZENIE 4: Ranking pracowników w dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: Window.partitionBy('department').orderBy(col('salary').desc())
        
        print("\nTwoje rozwiązanie DataFrame API:")
        window_spec = Window.partitionBy('department').orderBy(col('salary').desc())
        df_result = self.employees_df.select('name', 'department', 'salary', row_number().over(window_spec).alias('rank'))
        df_result.show()


        pass
    
    def exercise_5(self):
        """5. Znajdź pracowników z pensją wyższą niż średnia w ich dziale"""
        print("\n=== ĆWICZENIE 5: Pensja wyższa niż średnia w dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: Window function z avg() OVER (PARTITION BY department)
        
        window_spec = Window.partitionBy('department')
        df_result = self.employees_df.withColumn('dept_avg', round(avg('salary').over(window_spec),2)).filter(col('salary') > col('dept_avg'))
        df_result.show()
    
        

        pass
    
    def exercise_6(self):
        """6. Znajdź pracownika z najwyższą pensją w każdym dziale"""
        print("\n=== ĆWICZENIE 6: Najwyższa pensja w każdym dziale ===\n")
        
        # TODO: Użyj window function z RANK() = 1

        window_spec = Window.partitionBy('department').orderBy(col('salary').desc())
        df_result = self.employees_df.select('name', 'department', 'salary', row_number().over(window_spec).alias('rank')).filter(col('rank') == 1)
        df_result.show()

        print()
        print('Ten sam ranking, ale bez widocznej kolumny rank')
        df_result.select('name', 'department', 'salary').show()

        pass
    
    def exercise_7(self):
        """7. Oblicz różnicę między pensją pracownika a średnią w jego dziale"""
        print("\n=== ĆWICZENIE 7: Różnica od średniej w dziale ===\n")
        
        # TODO: salary - avg(salary) OVER (PARTITION BY department)

        window_spec = Window.partitionBy('department')
        df_result = self.employees_df.withColumn('difference rounded', round(col('salary') - avg('salary').over(window_spec)))
        df_result.show()

        print()
        print('jeszcze raz, ale inaczej')
        df_result = self.employees_df.select('*', (col('salary') - avg('salary').over(window_spec)).alias('diff'))
        df_result.show()
        print('i jeszcze jeden, ale ze zmienną diff_column')
        diff_column = (col('salary') - avg('salary').over(window_spec)).alias('diff')
        df_result = self.employees_df.select('*', diff_column)
        df_result.show()

        print('statystyki:')
        print('Ilość wierszy: ', df_result.count())
        print('Ilość kolumn: ',len(df_result.columns))
        print()
        print('używamy describe:')
        df_result.describe().show()
        print()
        print('używamy printSchema:')
        df_result.printSchema()

        pass
    
    def exercise_8(self):
        """8. Znajdź pracowników zatrudnionych jako ostatni w każdym dziale"""
        print("\n=== ĆWICZENIE 8: Ostatnio zatrudnieni w dziale ===\n")
        
        # TODO: Window function z ORDER BY hire_date DESC

        window_spec = Window.partitionBy('department').orderBy(col('hire_date').desc())
        df_result = self.employees_df.select('name', 'department', 'hire_date', row_number().over(window_spec).alias('rank')).filter(col('rank') == 1)
        df_result.show()

        pass
    
    def exercise_9(self):
        """9. Stwórz kategorię pensji: Low (<5000), Medium (5000-5500), High (>5500)"""
        print("\n=== ĆWICZENIE 9: Kategorie pensji ===\n")
        
        # TODO: Użyj CASE WHEN lub when().otherwise()
        df_result = self.employees_df.select('*').withColumn('salary_cat', when(col('salary') > 5500, 'High').when(col('salary') >= 5000, 'Medium').otherwise('Low'))
        df_result.show()

        print()
        print('bez selecta')
        df_result = self.employees_df.withColumn('salary_cat', when(col('salary') > 5500, 'High').when(col('salary') >= 5000, 'Medium').otherwise('Low'))
        df_result.show()

        

        pass
    
    def exercise_10(self):
        """10. Znajdź działy gdzie wszyscy pracownicy zarabiają powyżej 4600"""
        print("\n=== ĆWICZENIE 10: Działy z wysokimi pensjami ===\n")
        
        # TODO: GROUP BY + HAVING MIN(salary) > 4600

        df_result = self.employees_df.groupBy(col('department')).agg(min(col('salary')).alias('min_salary')).filter(col('min_salary') > 4600)
        df_result.show()
        pass

    def run_exercises(self):
        """Uruchom wszystkie ćwiczenia."""
        # self.exercise_0()
        # self.exercise_1()
        # self.exercise_2()
        # self.exercise_3()
        # self.exercise_4()
        # self.exercise_5()
        
        # print("\n" + "="*50)
        # print("NOWE TRUDNIEJSZE ĆWICZENIA")
        # print("="*50)
        
        # self.exercise_6()
        # self.exercise_7()
        #self.exercise_8()
        self.exercise_9()
        self.exercise_10()
        
        self.spark.stop()

if __name__ == "__main__":
    exercises = PySparkExercises()
    exercises.run_exercises()