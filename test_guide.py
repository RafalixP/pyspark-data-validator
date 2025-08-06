#!/usr/bin/env python3

from sql_to_pyspark_guide import SQLToPySparkGuide

if __name__ == "__main__":
    print("Starting SQL to PySpark Guide...")
    
    try:
        guide = SQLToPySparkGuide()
        print("\n" + "="*50)
        guide.basic_queries()
        
        print("\n" + "="*50)
        guide.aggregations()
        
        print("\n" + "="*50)
        guide.joins()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()