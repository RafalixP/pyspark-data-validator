"""
Przykład użycia PySpark Data Validator
"""

from pyspark_validator import PySparkDataValidator
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def main():
    # Inicjalizacja walidatora
    validator = PySparkDataValidator()
    spark = validator.spark
    
    # Tworzenie przykładowych danych
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True)
    ])
    
    data = [
        (1, "Jan Kowalski", "jan@example.com", 30, 5000.0),
        (2, "Anna Nowak", "anna.nowak@test.pl", 25, 4500.0),
        (3, "Piotr Wiśniewski", "invalid-email", 35, -1000.0),  # błędny email, ujemna pensja
        (1, "Jan Kowalski", "jan@example.com", 30, 5000.0),     # duplikat
        (4, None, "test@example.com", None, 6000.0),            # brakujące wartości
        (5, "Maria Kowalczyk", "maria@company.com", 28, 5500.0)
    ]
    
    df = spark.createDataFrame(data, schema)
    
    print("=== PRZYKŁADOWE DANE ===")
    df.show()
    
    # Test 1: Sprawdzanie duplikatów
    print("\n=== SPRAWDZANIE DUPLIKATÓW ===")
    duplicates_result = validator.check_duplicates(df)
    print(f"Duplikaty: {duplicates_result}")
    
    # Test 2: Sprawdzanie brakujących wartości
    print("\n=== BRAKUJĄCE WARTOŚCI ===")
    missing_result = validator.check_missing_values(df)
    print(f"Brakujące wartości: {missing_result}")
    
    # Test 3: Walidacja emaili
    print("\n=== WALIDACJA EMAILI ===")
    email_result = validator.validate_email_column(df, "email")
    print(f"Wyniki walidacji emaili: {email_result}")
    print("DataFrame z walidacją emaili:")
    email_result['validated_df'].select("name", "email", "email_valid").show()
    
    # Test 4: Sprawdzanie ujemnych wartości
    print("\n=== UJEMNE WARTOŚCI ===")
    negative_result = validator.check_negative_values(df, ["age", "salary"])
    print(f"Ujemne wartości: {negative_result}")
    
    # Test 5: Profilowanie danych
    print("\n=== PROFILOWANIE DANYCH ===")
    profiling_result = validator.data_profiling(df)
    print(f"Kolumny numeryczne: {profiling_result['numeric_columns']}")
    print("Statystyki:")
    for row in profiling_result['statistics']:
        print(row)
    
    # Test 6: Sprawdzanie zgodności schematu
    print("\n=== ZGODNOŚĆ SCHEMATU ===")
    expected_schema = {
        "id": "IntegerType",
        "name": "StringType", 
        "email": "StringType",
        "age": "IntegerType",
        "salary": "DoubleType"
    }
    schema_result = validator.check_schema_compliance(df, expected_schema)
    print(f"Zgodność schematu: {schema_result}")
    
    # Zamknięcie sesji
    validator.close()


if __name__ == "__main__":
    main()