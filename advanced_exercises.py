"""
Zaawansowane ćwiczenia PySpark - od podstaw do eksperckiego poziomu
15 ćwiczeń o rosnącym poziomie trudności
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

class AdvancedPySparkExercises:
    def __init__(self):
        # Windows-compatible configuration
        self.spark = SparkSession.builder \
            .appName("AdvancedPySparkExercises") \
            .master("local[1]") \
            .config("spark.driver.memory", "512m") \
            .config("spark.python.worker.reuse", "false") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        self.create_sample_data()
    
    def create_sample_data(self):
        """Tworzy rozszerzone dane testowe."""
        
        # Tabela employees - rozszerzona
        self.spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW employees AS
            SELECT * FROM VALUES 
                (1, 'Jan Kowalski', 'IT', 5000, '2020-01-15', 'Senior', 'Warsaw'),
                (2, 'Anna Nowak', 'HR', 4500, '2019-03-20', 'Junior', 'Krakow'),
                (3, 'Piotr Wiśniewski', 'IT', 6000, '2021-06-10', 'Senior', 'Warsaw'),
                (4, 'Maria Kowalczyk', 'Finance', 5500, '2020-11-05', 'Mid', 'Gdansk'),
                (5, 'Tomasz Zieliński', 'IT', 4800, '2022-02-28', 'Junior', 'Warsaw'),
                (6, 'Katarzyna Wójcik', 'HR', 5200, '2018-09-12', 'Senior', 'Krakow'),
                (7, 'Michał Kowalczyk', 'Finance', 4200, '2023-01-10', 'Junior', 'Gdansk'),
                (8, 'Agnieszka Nowak', 'IT', 7000, '2017-05-22', 'Expert', 'Warsaw'),
                (9, 'Robert Zieliński', 'Marketing', 4000, '2021-08-15', 'Mid', 'Poznan'),
                (10, 'Magdalena Kowal', 'Marketing', 3800, '2022-12-01', 'Junior', 'Poznan')
            AS t(id, name, department, salary, hire_date, level, city)
        """)
        
        # Tabela departments
        self.spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW departments AS
            SELECT * FROM VALUES 
                ('IT', 'Technology', 'Warsaw', 1000000),
                ('HR', 'Human Resources', 'Krakow', 200000),
                ('Finance', 'Financial', 'Gdansk', 500000),
                ('Marketing', 'Marketing', 'Poznan', 300000)
            AS t(dept_name, dept_full_name, location, budget)
        """)
        
        # Tabela projects
        self.spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW projects AS
            SELECT * FROM VALUES 
                (101, 'Website Redesign', 'IT', '2023-01-01', '2023-06-30', 'Active'),
                (102, 'HR System', 'IT', '2022-09-01', '2023-03-31', 'Completed'),
                (103, 'Budget Planning', 'Finance', '2023-02-01', '2023-12-31', 'Active'),
                (104, 'Marketing Campaign', 'Marketing', '2023-03-01', '2023-09-30', 'Active'),
                (105, 'Recruitment Drive', 'HR', '2023-01-15', '2023-04-15', 'Completed')
            AS t(project_id, project_name, department, start_date, end_date, status)
        """)
        
        # Tabela employee_projects (many-to-many)
        self.spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW employee_projects AS
            SELECT * FROM VALUES 
                (1, 101, 40), (3, 101, 30), (8, 101, 50),
                (1, 102, 20), (3, 102, 35), (8, 102, 45),
                (4, 103, 60), (7, 103, 40),
                (9, 104, 50), (10, 104, 30),
                (2, 105, 25), (6, 105, 35)
            AS t(employee_id, project_id, hours_allocated)
        """)
        
        # Tworzenie DataFrame
        self.employees_df = self.spark.table("employees")
        self.departments_df = self.spark.table("departments")
        self.projects_df = self.spark.table("projects")
        self.employee_projects_df = self.spark.table("employee_projects")
    
    def exercise_1(self):
        """1. PODSTAWY: Wybierz pracowników z pensją powyżej 5000"""
        print("=== ĆWICZENIE 1: Pracownicy z pensją > 5000 ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: użyj filter(col('salary') > 5000)
        
        df_result = self.employees_df.filter(col('salary') > 5000).orderBy(col('salary').asc())
        df_result.show()
    
    def exercise_2(self):
        """2. PODSTAWY: Policz liczbę pracowników w każdym dziale"""
        print("\n=== ĆWICZENIE 2: Liczba pracowników w dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: użyj groupBy('department').count()
     
        df_result = self.employees_df.groupBy(col('department')).count()
        df_result.show()
    
    def exercise_3(self):
        """3. PODSTAWY: Znajdź najwyższą i najniższą pensję w każdym mieście"""
        print("\n=== ĆWICZENIE 3: Min/Max pensja w mieście ===\n")

        df_result = self.employees_df.groupBy(col('city')).agg(max(col('salary')), min(col('salary')))
        df_result.show()

        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: groupBy('city').agg(max('salary'), min('salary'))
        
        pass
    
    def exercise_4(self):
        """4. ŚREDNI: Stwórz kategorię pensji (Low/Medium/High) używając CASE WHEN"""
        print("\n=== ĆWICZENIE 4: Kategorie pensji ===\n")

        df_result = self.employees_df.withColumn('salary_cat', when(col('salary') >= 6000, 'High').when(col('salary') >= 5000, 'Medium').otherwise('Low')).orderBy(col('salary_cat').desc())
        df_result.show()





        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: when(col('salary') < 4500, 'Low').when(col('salary') < 5500, 'Medium').otherwise('High')
        
        pass
    
    def exercise_5(self):
        """5. ŚREDNI: Znajdź pracowników zatrudnionych w ostatnich 4 latach"""
        print("\n=== ĆWICZENIE 5: Pracownicy z ostatnich 4 lat ===\n")

        df_result = self.employees_df.withColumn('date_difference', datediff(current_date(), col('hire_date'))).filter(col('date_difference') <= 1460)
        df_result.show()

        print()
        print('to samo, ale bez kolumny')
        df_result = self.employees_df.filter(datediff(current_date(), col('hire_date')) <= 1460)
        df_result.show()


        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: użyj datediff(current_date(), col('hire_date')) <= 730
        
        pass
    
    def exercise_6(self):
        """6. ŚREDNI: Oblicz średnią pensję dla każdego poziomu (level) w każdym dziale"""
        print("\n=== ĆWICZENIE 6: Średnia pensja według poziomu i działu ===\n")
        
        # df_result = self.employees_df.groupBy(col('level')).agg(avg(col('salary')))
        # df_result.show()

        df_result = self.employees_df.groupBy(col('department'), col('level')).agg(avg(col('salary'))).orderBy(col('department'))
        df_result.show()

        df_result = self.employees_df.groupBy(col('department'), col('level')).agg(avg(col('salary'))).orderBy(col('level'))
        df_result.show()


        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: groupBy('department', 'level').agg(avg('salary'))
        
        pass
    
    def exercise_7(self):
        """7. ŚREDNI: JOIN - Połącz pracowników z informacjami o działach"""
        print("\n=== ĆWICZENIE 7: JOIN pracowników z działami ===\n")

        df_result = self.employees_df.join(self.departments_df, col('department') == col('dept_name')).select('name', 'department', 'dept_name')
        df_result.show()
        
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: employees_df.join(departments_df, col('department') == col('dept_name'))
        
        pass
    
    def exercise_8(self):
        """8. ZAAWANSOWANY: Window Function - Ranking pensji w każdym dziale"""
        print("\n=== ĆWICZENIE 8: Ranking pensji w dziale ===\n")
        
        window_spec = Window.partitionBy('department').orderBy(col('salary').desc())
        df_result = self.employees_df.select('*', row_number().over(window_spec).alias('rank'))
        df_result.show()


        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: Window.partitionBy('department').orderBy(col('salary').desc())
        
        pass
    
    def exercise_9(self):
        """9. ZAAWANSOWANY: Oblicz różnicę pensji każdego pracownika od średniej w jego dziale"""
        print("\n=== ĆWICZENIE 9: Różnica od średniej działu ===\n")

        window_spec = Window.partitionBy('department')
        df_result = self.employees_df.withColumn('dept_avg', avg('salary').over(window_spec)).withColumn('salary_diff', col('dept_avg')-col('salary'))
        df_result.show()

        df_result = self.employees_df.withColumn('salary_diff', avg('salary').over(window_spec)-col('salary'))
        df_result.show()


        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: withColumn('dept_avg', avg('salary').over(window)).withColumn('diff', col('salary') - col('dept_avg'))
        
        pass
    
    def exercise_10(self):
        """10. ZAAWANSOWANY: Znajdź pracowników, którzy zarabiają więcej niż poprzednik w rankingu"""
        print("\n=== ĆWICZENIE 10: Porównanie z poprzednikiem ===\n")



        window_spec = Window.partitionBy('department').orderBy('salary')
        df_result = self.employees_df.withColumn('wartosc_poprzednia', lag('salary').over(window_spec)).withColumn('earns_more', col('salary') > col('wartosc_poprzednia'))
        df_result.show()

        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: użyj lag(col('salary')).over(window) do porównania z poprzednią wartością
        
        pass
    
    def exercise_11(self):
        """11. EKSPERT: Kompleksowy JOIN - Pracownicy, projekty i godziny"""
        print("\n=== ĆWICZENIE 11: Kompleksowy JOIN trzech tabel ===\n")

        df_result = self.employees_df.join(self.employee_projects_df, col('id') == col('employee_id')).join(self.projects_df, ['project_id'])
        df_result.show()

        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: Połącz employees -> employee_projects -> projects
        
        pass
    
    def exercise_12(self):
        """12. EKSPERT: Pivot - Przekształć dane o projektach na kolumny"""
        print("\n=== ĆWICZENIE 12: Pivot projektów ===\n")
        
        df_result = self.employee_projects_df.join(self.employees_df, col('employee_id') == col('id') ).groupBy('name').pivot('project_id').agg(sum('hours_allocated'))
        df_result.show()
        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: użyj pivot() żeby projekty stały się kolumnami z godzinami
        
        pass
    
    def exercise_13(self):
        """13. EKSPERT: Analiza kohort - Grupuj pracowników według roku zatrudnienia"""
        print("\n=== ĆWICZENIE 13: Analiza kohort zatrudnienia ===\n")

        
        df_result = self.employees_df.withColumn('hire_year', year(col('hire_date'))).groupBy('hire_year').count()
        df_result.show()

        df_result = self.employees_df.withColumn('hire_year', year(col('hire_date'))) #.groupBy('hire_year').count()
        df_result.show()

        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: year(col('hire_date')), percentile_approx, collect_list
        
        pass
    
    def exercise_14(self):
        """14. EKSPERT: Rekurencyjne obliczenia - Skumulowana suma pensji"""
        print("\n=== ĆWICZENIE 14: Running total pensji ===\n")

        window_spec = Window.orderBy(col('salary')).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        df_result = self.employees_df.withColumn('running_total', sum('salary').over(window_spec))
        df_result.show()

        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: sum().over(Window.orderBy().rowsBetween(Window.unboundedPreceding, Window.currentRow))
        
        pass
    
    def exercise_15(self):
        """15. MISTRZ: Zaawansowana analityka - Top N w każdej grupie z dodatkowymi warunkami"""
        print("\n=== ĆWICZENIE 15: Top 2 najlepiej płatnych w każdym dziale z dodatkowymi warunkami ===\n")
        
        window_spec = Window.partitionBy('department').orderBy(col('salary').desc())
        df_result = self.employees_df.withColumn('rank', row_number().over(window_spec)).filter(col('rank') <= 2)
        df_result.show()




















        # TODO: Napisz rozwiązanie tutaj
        # Wskazówka: Połącz window functions, filtering, i complex conditions
        # Znajdź top 2 najlepiej płatnych w każdym dziale, ale tylko tych zatrudnionych po 2019
        # i pracujących w projektach aktywnych
        
        pass
    
    def run_exercises(self):
        """Uruchom wszystkie ćwiczenia."""
        print("🚀 ZAAWANSOWANE ĆWICZENIA PYSPARK - 15 ZADAŃ 🚀\n")
        print("Poziomy trudności:")
        print("📚 PODSTAWY (1-3): Podstawowe operacje")
        print("📈 ŚREDNI (4-7): Funkcje, JOIN'y, grupowanie")
        print("🔥 ZAAWANSOWANY (8-10): Window functions, analityka")
        print("🎯 EKSPERT (11-13): Kompleksowe JOIN'y, pivot, kohorty")
        print("👑 MISTRZ (14-15): Zaawansowana analityka\n")
        
        exercises = [
            self.exercise_1, self.exercise_2, self.exercise_3, self.exercise_4, self.exercise_5,
            self.exercise_6, self.exercise_7, self.exercise_8, self.exercise_9, self.exercise_10,
            self.exercise_11, self.exercise_12, self.exercise_13, self.exercise_14, self.exercise_15
        ]
        
        for exercise in exercises:
            exercise()

        # self.exercise_1()

        
        print("\n🎉 Gratulacje! Ukończyłeś wszystkie 15 ćwiczeń! 🎉")
        self.spark.stop()

if __name__ == "__main__":
    exercises = AdvancedPySparkExercises()
    exercises.run_exercises() 