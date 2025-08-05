# PySpark Data Quality Validator

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Projekt demonstrujący walidację jakości danych w PySpark - od podstawowych funkcji pandas do zaawansowanych pipeline'ów big data.

## 🎯 Cel projektu

Przygotowanie do pozycji **Test Engineer – Data & AI** poprzez praktyczną naukę:
- Przepisywania funkcji pandas na PySpark
- Testowania pipeline'ów danych
- Optymalizacji wydajności big data
- Implementacji CI/CD dla jakości danych

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

## Scenariusze testowe dla rozmowy

1. **Big Data Processing**: Jak obsłużyć DataFrame z milionami rekordów?
2. **Performance Tuning**: Optymalizacja zapytań i partycjonowania
3. **Data Quality Pipelines**: Automatyzacja walidacji w CI/CD
4. **Schema Evolution**: Obsługa zmian w schemacie danych
5. **Anomaly Detection**: Wykrywanie nietypowych wzorców w danych

## Kluczowe różnice PySpark vs Pandas

| Aspekt | Pandas | PySpark |
|--------|--------|---------|
| Przetwarzanie | Single-machine | Distributed |
| Lazy Evaluation | Nie | Tak |
| Memory Management | In-memory | Disk + Memory |
| SQL Support | Ograniczone | Pełne wsparcie |
| Scalability | Ograniczona | Nieograniczona |

## 🤝 Contributing

Chętnie przyjmę pull requesty! Szczególnie mile widziane:
- Nowe funkcje walidacyjne
- Optymalizacje wydajności
- Dodatkowe testy
- Przykłady integracji z Azure/AWS

## 📧 Kontakt

Rafał Pieczka - [LinkedIn](https://linkedin.com/in/rafal-pieczka)

## 📄 Licencja

Projekt jest dostępny na licencji MIT - zobacz [LICENSE](LICENSE) dla szczegółów.