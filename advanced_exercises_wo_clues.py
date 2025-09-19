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
            .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse") \
            .config("spark.local.dir", "c:/tmp/spark-temp") \
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
        """1. Wybierz pracowników z pensją powyżej 5000"""
        print("=== ĆWICZENIE 1: Pracownicy z pensją > 5000 ===\n")
        
        # TODO: Napisz rozwiązanie tutaj

        df_result = self.employees_df.filter(col('salary')>5000).orderBy(col('salary').desc())
        df_result.show()
        
        pass
    
    def exercise_2(self):
        """2. Policz liczbę pracowników w każdym dziale"""
        print("\n=== ĆWICZENIE 2: Liczba pracowników w dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        df_result = self.employees_df.groupBy(col('department')).count()
        df_result.show()
        
        pass
    
    def exercise_3(self):
        """3. Znajdź najwyższą i najniższą pensję w każdym mieście"""
        print("\n=== ĆWICZENIE 3: Min/Max pensja w mieście ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        df_result = self.employees_df.groupBy(col('city')).agg(max(col('salary')),min(col('salary')),avg(col('salary')))
        df_result.show()
        pass
    
    def exercise_3a(self):
        """3A. Oblicz medianę pensji dla każdego działu używając percentile_approx"""
        print("\n=== ĆWICZENIE 3A: Mediana pensji w dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        df_result = self.employees_df.groupBy(col('department')).agg(percentile_approx(col('salary'),0.5))
        df_result.show()

        pass
    
    
    def exercise_4(self):
        """4. Stwórz kategorię pensji (Low/Medium/High) używając CASE WHEN"""
        print("\n=== ĆWICZENIE 4: Kategorie pensji ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_5(self):
        """5. Znajdź pracowników zatrudnionych w ostatnich 4 latach"""
        print("\n=== ĆWICZENIE 5: Pracownicy z ostatnich 4 lat ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_5a(self):
        """5A. Przekształć liczbę pracowników według działu i poziomu na tabelę przestawną"""
        print("\n=== ĆWICZENIE 5A: Pivot działy vs poziomy ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass

    
    def exercise_6(self):
        """6. Oblicz średnią pensję dla każdego poziomu (level) w każdym dziale"""
        print("\n=== ĆWICZENIE 6: Średnia pensja według poziomu i działu ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_7(self):
        """7. JOIN - Połącz pracowników z informacjami o działach"""
        print("\n=== ĆWICZENIE 7: JOIN pracowników z działami ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_7a(self):
        """7A. Znajdź pracowników bez przypisanych projektów używając LEFT ANTI JOIN"""
        print("\n=== ĆWICZENIE 7A: Pracownicy bez projektów ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    

    
    def exercise_8(self):
        """8. Window Function - Ranking pensji w każdym dziale"""
        print("\n=== ĆWICZENIE 8: Ranking pensji w dziale ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_9(self):
        """9. Oblicz różnicę pensji każdego pracownika od średniej w jego dziale"""
        print("\n=== ĆWICZENIE 9: Różnica od średniej działu ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_9a(self):
        """9A. Stwórz macierz pracowników vs miasta z ich średnimi pensjami"""
        print("\n=== ĆWICZENIE 9A: Pivot pracownicy vs miasta z pensjami ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_10(self):
        """10. Znajdź pracowników, którzy zarabiają więcej niż poprzednik w rankingu"""
        print("\n=== ĆWICZENIE 10: Porównanie z poprzednikiem ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_11(self):
        """11. Kompleksowy JOIN - Pracownicy, projekty i godziny"""
        print("\n=== ĆWICZENIE 11: Kompleksowy JOIN trzech tabel ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_11a(self):
        """11A. Stwórz ranking pracowników według łącznej liczby godzin w projektach z percentylami"""
        print("\n=== ĆWICZENIE 11A: Ranking godzin z percentylami ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_12(self):
        """12. Pivot - Przekształć dane o projektach na kolumny"""
        print("\n=== ĆWICZENIE 12: Pivot projektów ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_13(self):
        """13. Analiza kohort - Grupuj pracowników według roku zatrudnienia"""
        print("\n=== ĆWICZENIE 13: Analiza kohort zatrudnienia ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    

    
    def exercise_14(self):
        """14. Rekurencyjne obliczenia - Skumulowana suma pensji"""
        print("\n=== ĆWICZENIE 14: Running total pensji ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_14a(self):
        """14A. Utwórz kompleksową macierz projektów vs statusów z sumą godzin i średnią pensją pracowników"""
        print("\n=== ĆWICZENIE 14A: Pivot projekty vs statusy z metrykami ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
        pass
    
    def exercise_15(self):
        """15. Zaawansowana analityka - Top N w każdej grupie z dodatkowymi warunkami"""
        print("\n=== ĆWICZENIE 15: Top 2 najlepiej płatnych w każdym dziale z dodatkowymi warunkami ===\n")
        
        # TODO: Napisz rozwiązanie tutaj
        
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
            self.exercise_1, self.exercise_2, self.exercise_3, self.exercise_3a, self.exercise_4, self.exercise_5, self.exercise_5a,
            self.exercise_6, self.exercise_7, self.exercise_7a, self.exercise_8, self.exercise_9, self.exercise_9a, self.exercise_10,
            self.exercise_11, self.exercise_11a, self.exercise_12, self.exercise_13, self.exercise_14, self.exercise_14a, self.exercise_15
        ]
        
        for exercise in exercises:
            exercise()
        
        print("\n🎉 Gratulacje! Ukończyłeś wszystkie 15 ćwiczeń! 🎉")
        self.spark.stop()

    def validate_data_quality(self):
        """Comprehensive data quality validation suite"""
        print("\n=== DATA QUALITY VALIDATION SUITE ===\n")
        
        # Schema validation
        expected_columns = ['id', 'name', 'department', 'salary', 'hire_date', 'level', 'city']
        actual_columns = self.employees_df.columns
        assert set(expected_columns) == set(actual_columns), f"Schema mismatch. Expected: {expected_columns}, Got: {actual_columns}"
        
        # Data type validation
        schema_dict = {field.name: str(field.dataType) for field in self.employees_df.schema.fields}
        assert 'int' in schema_dict['id'].lower(), "ID should be integer type"
        assert 'string' in schema_dict['name'].lower(), "Name should be string type"
        
        # Null value checks
        for col_name in ['id', 'name', 'department', 'salary']:
            null_count = self.employees_df.filter(col(col_name).isNull()).count()
            assert null_count == 0, f"Column {col_name} should not have null values"
        
        # Business rule validations
        assert self.employees_df.filter(col('salary') < 0).count() == 0, "Salary should be positive"
        assert self.employees_df.filter(col('id') <= 0).count() == 0, "ID should be positive"
        
        # Duplicate checks
        total_count = self.employees_df.count()
        unique_id_count = self.employees_df.select('id').distinct().count()
        assert total_count == unique_id_count, "Employee IDs should be unique"
        
        print("✅ All data quality validations passed!")

if __name__ == "__main__":
    exercises = AdvancedPySparkExercises()
    exercises.validate_data_quality()
    exercises.run_exercises()