# PySpark Data Quality Validator

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Projekt demonstrujący walidację jakości danych w PySpark - od podstawowych funkcji pandas do zaawansowanych pipeline'ów big data.

## 🎯 Cel projektu

Nauka:
- Przepisywania funkcji pandas na PySpark
- Testowania pipeline'ów danych
- Optymalizacji wydajności big data
- Implementacji CI/CD dla jakości danych

## 👥 Who is this for?

- **Data Engineers** learning PySpark data validation
- **QA Engineers** transitioning to big data testing
- **Data Scientists** needing data quality checks
- **Students** exploring distributed data processing

## Prerequisites

- Python 3.8+
- Basic understanding of pandas
- Familiarity with data quality concepts
- (Optional) Spark cluster for distributed testing

## Struktura projektu

```
pyspark-data-validator/
├── requirements.txt          # Zależności
├── pyspark_validator.py      # Główna klasa walidatora
├── example_usage.py          # Podstawowe przykłady użycia
├── advanced_examples.py      # Zaawansowane scenariusze
├── test_pyspark_validator.py # Testy jednostkowe
└── README.md                # Ten plik
```

## Instalacja

```bash
pip install -r requirements.txt
```

## Uruchomienie

### Podstawowe przykłady
```bash
python example_usage.py
```

### Zaawansowane funkcje
```bash
python advanced_examples.py
```

### Testy
```bash
pytest test_pyspark_validator.py -v
```

## Kluczowe funkcjonalności

### 1. Walidacja jakości danych
- Sprawdzanie duplikatów
- Wykrywanie brakujących wartości
- Walidacja formatów (email, telefon)
- Sprawdzanie ujemnych wartości
- Zgodność schematu

### 2. Zaawansowane funkcje
- Pipeline walidacji jakości
- Wykrywanie anomalii (IQR)
- Śledzenie pochodzenia danych
- Analiza w oknach przesuwnych
- Strategie próbkowania

### 3. Optymalizacja wydajności
- Partycjonowanie danych
- Cache'owanie DataFrame
- Monitorowanie wykonania

## Przykład użycia

```python
from pyspark_validator import PySparkDataValidator

# Inicjalizacja
validator = PySparkDataValidator()

# Sprawdzenie duplikatów
result = validator.check_duplicates(df, columns=["id"])
print(f"Duplikaty: {result['duplicate_count']}")

# Walidacja emaili
email_result = validator.validate_email_column(df, "email")
print(f"Poprawne emaile: {email_result['validation_rate']}%")

# Zamknięcie
validator.close()
```

## Use Cases

1. **Big Data Processing**: Handling DataFrames with millions of records
2. **Performance Tuning**: Query and partitioning optimization
3. **Data Quality Pipelines**: Automated validation in CI/CD
4. **Schema Evolution**: Managing data schema changes
5. **Anomaly Detection**: Identifying unusual patterns in data

## Kluczowe różnice PySpark vs Pandas

| Aspekt | Pandas | PySpark |
|--------|--------|---------|
| Przetwarzanie | Single-machine | Distributed |
| Lazy Evaluation | Nie | Tak |
| Memory Management | In-memory | Disk + Memory |
| SQL Support | Ograniczone | Pełne wsparcie |
| Scalability | Ograniczona | Nieograniczona |

## 🤝 Contributing

Pull requests are welcome! Areas of interest:
- New validation functions
- Performance optimizations
- Additional test cases
- Cloud platform integrations (Azure/AWS/GCP)

## 📧 Kontakt

Rafał Pieczka - [LinkedIn](https://linkedin.com/in/rafal-pieczka)

## 📄 Licencja

Projekt jest dostępny na licencji MIT - zobacz [LICENSE](LICENSE) dla szczegółów.