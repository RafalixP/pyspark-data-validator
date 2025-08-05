"""
PySpark Data Quality Validator - przepisane funkcje z pandas na PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, regexp_extract, sum as spark_sum
from pyspark.sql.types import *
import re


class PySparkDataValidator:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DataQualityValidator") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def check_duplicates(self, df, columns=None):
        """Sprawdza duplikaty w PySpark DataFrame."""
        total_rows = df.count()
        
        if columns:
            distinct_rows = df.dropDuplicates(columns).count()
        else:
            distinct_rows = df.dropDuplicates().count()
        
        duplicate_count = total_rows - distinct_rows
        
        return {
            'has_duplicates': duplicate_count > 0,
            'duplicate_count': duplicate_count,
            'total_rows': total_rows,
            'unique_rows': distinct_rows
        }
    
    def check_missing_values(self, df):
        """Sprawdza brakujące wartości w PySpark DataFrame."""
        total_rows = df.count()
        missing_counts = {}
        missing_percentages = {}
        
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            missing_counts[column] = null_count
            missing_percentages[column] = (null_count / total_rows) * 100 if total_rows > 0 else 0
        
        return {
            'missing_by_column': missing_counts,
            'missing_percent': missing_percentages,
            'total_missing': sum(missing_counts.values())
        }
    
    def validate_email_column(self, df, email_column):
        """Waliduje kolumnę email w PySpark DataFrame."""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        result_df = df.withColumn(
            f"{email_column}_valid",
            when(col(email_column).rlike(email_pattern), True).otherwise(False)
        )
        
        valid_count = result_df.filter(col(f"{email_column}_valid") == True).count()
        total_count = result_df.count()
        
        return {
            'valid_emails': valid_count,
            'invalid_emails': total_count - valid_count,
            'validation_rate': (valid_count / total_count) * 100 if total_count > 0 else 0,
            'validated_df': result_df
        }
    
    def check_negative_values(self, df, numeric_columns):
        """Sprawdza ujemne wartości w kolumnach numerycznych."""
        results = {}
        
        for column in numeric_columns:
            if column in df.columns:
                negative_count = df.filter(col(column) < 0).count()
                total_count = df.filter(col(column).isNotNull()).count()
                
                results[column] = {
                    'negative_count': negative_count,
                    'total_count': total_count,
                    'has_negatives': negative_count > 0,
                    'negative_percentage': (negative_count / total_count) * 100 if total_count > 0 else 0
                }
        
        return results
    
    def data_profiling(self, df):
        """Podstawowe profilowanie danych - statystyki opisowe."""
        numeric_columns = [field.name for field in df.schema.fields 
                          if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType))]
        
        if not numeric_columns:
            return {"message": "Brak kolumn numerycznych do profilowania"}
        
        stats_df = df.select(numeric_columns).describe()
        return {
            'numeric_columns': numeric_columns,
            'statistics': stats_df.collect()
        }
    
    def check_schema_compliance(self, df, expected_schema):
        """Sprawdza zgodność schematu DataFrame z oczekiwanym."""
        actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
        
        compliance_results = {}
        for column, expected_type in expected_schema.items():
            if column in actual_schema:
                compliance_results[column] = {
                    'expected': expected_type,
                    'actual': actual_schema[column],
                    'compliant': expected_type in actual_schema[column]
                }
            else:
                compliance_results[column] = {
                    'expected': expected_type,
                    'actual': 'MISSING',
                    'compliant': False
                }
        
        return compliance_results
    
    def close(self):
        """Zamyka sesję Spark."""
        self.spark.stop()