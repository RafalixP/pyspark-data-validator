"""
Testy jednostkowe dla PySpark Data Validator
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark_validator import PySparkDataValidator


@pytest.fixture(scope="session")
def spark_session():
    """Fixture dla sesji Spark."""
    spark = SparkSession.builder \
        .appName("TestDataValidator") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def validator(spark_session):
    """Fixture dla walidatora."""
    validator = PySparkDataValidator()
    validator.spark = spark_session  # Użyj test session
    yield validator


@pytest.fixture
def sample_df(spark_session):
    """Fixture z przykładowymi danymi."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True)
    ])
    
    data = [
        (1, "Jan Kowalski", "jan@example.com", 30, 5000.0),
        (2, "Anna Nowak", "anna@test.pl", 25, 4500.0),
        (3, "Piotr Wiśniewski", "invalid-email", 35, -1000.0),
        (1, "Jan Kowalski", "jan@example.com", 30, 5000.0),  # duplikat
        (4, None, "test@example.com", None, 6000.0)
    ]
    
    return spark_session.createDataFrame(data, schema)


class TestPySparkDataValidator:
    
    def test_check_duplicates_with_columns(self, validator, sample_df):
        """Test sprawdzania duplikatów z określonymi kolumnami."""
        result = validator.check_duplicates(sample_df, columns=["id", "name"])
        
        assert result['has_duplicates'] == True
        assert result['duplicate_count'] == 1
        assert result['total_rows'] == 5
        assert result['unique_rows'] == 4
    
    def test_check_duplicates_all_columns(self, validator, sample_df):
        """Test sprawdzania duplikatów wszystkich kolumn."""
        result = validator.check_duplicates(sample_df)
        
        assert result['has_duplicates'] == True
        assert result['duplicate_count'] == 1
    
    def test_check_missing_values(self, validator, sample_df):
        """Test sprawdzania brakujących wartości."""
        result = validator.check_missing_values(sample_df)
        
        assert result['missing_by_column']['name'] == 1
        assert result['missing_by_column']['age'] == 1
        assert result['missing_by_column']['id'] == 0
        assert result['missing_percent']['name'] == 20.0
    
    def test_validate_email_column(self, validator, sample_df):
        """Test walidacji kolumny email."""
        result = validator.validate_email_column(sample_df, "email")
        
        assert result['valid_emails'] == 3
        assert result['invalid_emails'] == 2
        assert result['validation_rate'] == 60.0
        assert 'validated_df' in result
    
    def test_check_negative_values(self, validator, sample_df):
        """Test sprawdzania ujemnych wartości."""
        result = validator.check_negative_values(sample_df, ["age", "salary"])
        
        assert result['salary']['negative_count'] == 1
        assert result['salary']['has_negatives'] == True
        assert result['age']['negative_count'] == 0
        assert result['age']['has_negatives'] == False
    
    def test_data_profiling(self, validator, sample_df):
        """Test profilowania danych."""
        result = validator.data_profiling(sample_df)
        
        assert 'numeric_columns' in result
        assert 'statistics' in result
        assert len(result['numeric_columns']) == 3  # id, age, salary
    
    def test_check_schema_compliance(self, validator, sample_df):
        """Test sprawdzania zgodności schematu."""
        expected_schema = {
            "id": "IntegerType",
            "name": "StringType",
            "email": "StringType",
            "missing_column": "StringType"
        }
        
        result = validator.check_schema_compliance(sample_df, expected_schema)
        
        assert result['id']['compliant'] == True
        assert result['name']['compliant'] == True
        assert result['missing_column']['compliant'] == False
        assert result['missing_column']['actual'] == 'MISSING'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])