"""
PySpark Concepts - Nauka bez uruchamiania Spark
Pokazuje różnice między pandas a PySpark na przykładach kodu
"""

def show_pandas_vs_pyspark_examples():
    """Porównanie składni pandas vs PySpark bez uruchamiania."""
    
    print("=== PANDAS VS PYSPARK - PORÓWNANIE SKŁADNI ===\n")
    
    # 1. Podstawowe operacje
    print("1. PODSTAWOWE OPERACJE")
    print("PANDAS:")
    print("  df = pd.read_csv('data.csv')")
    print("  df.head()")
    print("  df['column'].sum()")
    print()
    print("PYSPARK:")
    print("  df = spark.read.csv('data.csv', header=True)")
    print("  df.show(5)")
    print("  df.agg(sum('column')).collect()")
    print()
    
    # 2. Filtrowanie
    print("2. FILTROWANIE")
    print("PANDAS:")
    print("  df[df['age'] > 25]")
    print("  df.query('age > 25 and salary > 5000')")
    print()
    print("PYSPARK:")
    print("  df.filter(col('age') > 25)")
    print("  df.filter((col('age') > 25) & (col('salary') > 5000))")
    print()
    
    # 3. Grupowanie
    print("3. GRUPOWANIE")
    print("PANDAS:")
    print("  df.groupby('department').agg({'salary': 'mean'})")
    print()
    print("PYSPARK:")
    print("  df.groupBy('department').agg(avg('salary'))")
    print()
    
    # 4. Nowe kolumny
    print("4. NOWE KOLUMNY")
    print("PANDAS:")
    print("  df['bonus'] = df['salary'] * 0.1")
    print("  df['category'] = df['salary'].apply(lambda x: 'High' if x > 5000 else 'Low')")
    print()
    print("PYSPARK:")
    print("  df.withColumn('bonus', col('salary') * 0.1)")
    print("  df.withColumn('category', when(col('salary') > 5000, 'High').otherwise('Low'))")
    print()
    
    # 5. JOIN
    print("5. JOIN")
    print("PANDAS:")
    print("  pd.merge(df1, df2, on='id', how='inner')")
    print()
    print("PYSPARK:")
    print("  df1.join(df2, df1.id == df2.id, 'inner')")
    print()
    
    # 6. SQL
    print("6. SQL")
    print("PANDAS:")
    print("  # Brak natywnego wsparcia SQL")
    print()
    print("PYSPARK:")
    print("  df.createOrReplaceTempView('employees')")
    print("  spark.sql('SELECT * FROM employees WHERE salary > 5000')")
    print()

def show_key_differences():
    """Pokazuje kluczowe różnice między pandas a PySpark."""
    
    print("=== KLUCZOWE RÓŻNICE ===\n")
    
    differences = [
        ("Wykonanie", "pandas: Eager (natychmiastowe)", "PySpark: Lazy (opóźnione)"),
        ("Pamięć", "pandas: Wszystko w RAM", "PySpark: Disk + Memory"),
        ("Skala", "pandas: Do ~10GB", "PySpark: Terabajty+"),
        ("Równoległość", "pandas: Ograniczona", "PySpark: Pełna paralelizacja"),
        ("SQL", "pandas: Brak", "PySpark: Natywne wsparcie"),
        ("Błędy", "pandas: Natychmiast", "PySpark: Przy wykonaniu (.show(), .collect())"),
        ("Optymalizacja", "pandas: Manualna", "PySpark: Automatyczna (Catalyst)")
    ]
    
    for aspect, pandas_way, pyspark_way in differences:
        print(f"{aspect}:")
        print(f"  📊 {pandas_way}")
        print(f"  ⚡ {pyspark_way}")
        print()

def show_when_to_use():
    """Pokazuje kiedy używać pandas vs PySpark."""
    
    print("=== KIEDY UŻYWAĆ KTÓREGO? ===\n")
    
    print("🐼 PANDAS - gdy:")
    print("  • Dane < 5GB")
    print("  • Szybkie prototypowanie")
    print("  • Analiza eksploracyjna")
    print("  • Praca lokalna")
    print("  • Potrzebujesz wszystkich funkcji pandas")
    print()
    
    print("⚡ PYSPARK - gdy:")
    print("  • Dane > 10GB")
    print("  • Produkcja/pipeline'y")
    print("  • Klaster Spark dostępny")
    print("  • Integracja z big data ecosystem")
    print("  • Potrzebujesz skalowalności")
    print()

def show_learning_path():
    """Pokazuje ścieżkę nauki PySpark dla użytkowników pandas."""
    
    print("=== ŚCIEŻKA NAUKI PANDAS → PYSPARK ===\n")
    
    steps = [
        "1. Zrozum lazy evaluation - operacje budują plan, nie wykonują się od razu",
        "2. Naucz się .show() i .collect() - to wywołuje wykonanie",
        "3. Opanuj DataFrame API - podobny do pandas, ale z col() i when()",
        "4. Poznaj Spark SQL - możesz używać znanego SQL",
        "5. Zrozum partycjonowanie - jak dane są dzielone",
        "6. Naucz się cache() - przechowywanie w pamięci",
        "7. Opanuj window functions - analiza w oknach",
        "8. Poznaj broadcast joins - optymalizacja JOIN'ów"
    ]
    
    for step in steps:
        print(f"  {step}")
    print()

def main():
    """Główna funkcja demonstracyjna."""
    show_pandas_vs_pyspark_examples()
    show_key_differences()
    show_when_to_use()
    show_learning_path()
    
    print("=== NASTĘPNE KROKI ===")
    print("1. Zainstaluj Java (OpenJDK 11 lub 17)")
    print("2. Uruchom sql_to_pyspark_guide.py")
    print("3. Eksperymentuj z example_usage.py")
    print("4. Rozwiąż ćwiczenia w sql_exercises.py")

if __name__ == "__main__":
    main()