"""
Data Engineering Assessment Solution
Converted from pandas to PySpark with optimizations and modularization
"""

import os
import json
import logging
import datetime
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Tuple, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, lit
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

# AWS and database imports
import boto3
from sqlalchemy import create_engine, inspect, text
from s3fs import S3FileSystem

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataProcessor:
    """Main class to handle all data processing operations using PySpark"""
    
    def __init__(self, config: Dict):
        """Initialize the DataProcessor with configuration"""
        self.config = config
        self.spark = self._initialize_spark_session()
        self.s3 = S3FileSystem()
        self.engine = self._create_db_connection()
        
    def _initialize_spark_session(self) -> SparkSession:
        """Initialize and configure Spark session with optimizations"""
        return SparkSession.builder \
            .appName("DataEngineeringAssessment") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.hadoop.fs.s3a.access.key", self.config.get('aws_access_key', '')) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config.get('aws_secret_key', '')) \
            .getOrCreate()
    
    def _create_db_connection(self):
        """Create database connection using SQLAlchemy"""
        db_config = self.config['database']
        user_encoded = quote_plus(db_config['user'])
        password_encoded = quote_plus(db_config['password'])
        conn_string = f"postgresql+psycopg2://{user_encoded}:{password_encoded}@{db_config['host']}/{db_config['dbname']}"
        return create_engine(conn_string)
    
    def load_data(self, file_path: str, file_type: str) -> DataFrame:
        """Load data from file (CSV or Parquet) into Spark DataFrame"""
        try:
            if file_type == 'csv':
                df = self.spark.read.csv(file_path, header=True, inferSchema=True)
            elif file_type == 'parquet':
                df = self.spark.read.parquet(file_path)
            else:
                raise ValueError('Unsupported file type')
            
            if 'datetime' in df.columns:
                df = df.withColumn('datetime', to_timestamp(col('datetime')))
            return df
            
        except Exception as e:
            logging.error(f"Error loading data from {file_path}: {str(e)}")
            raise
    
    def filter_tables(self, tables: List[str], year: int = 2024, start_month: int = 8) -> List[str]:
        """Filter tables based on year and month in their names"""
        filtered_tables = []
        for table in tables:
            parts = table.split('_')
            if len(parts) > 2 and parts[-2].isdigit() and parts[-1].isdigit():
                table_year = int(parts[-2])
                table_month = int(parts[-1])
                if table_year > year or (table_year == year and table_month >= start_month):
                    filtered_tables.append(table)
        return filtered_tables
    
    def execute_query(self, query: str) -> DataFrame:
        """Execute SQL query and return results as Spark DataFrame"""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                pandas_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return self.spark.createDataFrame(pandas_df)
        except Exception as e:
            logging.error(f"Failed to execute query: {str(e)}")
            raise
    
    def get_ids_all(self, tags: DataFrame, patterns: List[str]) -> str:
        """Get IDs from tags DataFrame that match any of the patterns"""
        pandas_tags = tags.toPandas()
        ids = []
        
        for pattern in patterns:
            logging.info(f"Pattern: {pattern}")
            energy = pandas_tags[pandas_tags['tagpath'].str.contains(pattern, case=False)].copy()
            logging.info(f"Matches Found: {energy.shape}")
            ids.extend(energy['id'].tolist())
            
        return ', '.join(map(str, set(ids)))
    
    def get_data_all(self, tables: List[str], tags: DataFrame, patterns: List[str], unix_ms: int) -> DataFrame:
        """Get data from multiple tables for matched tag IDs"""
        ids_str = self.get_ids_all(tags, patterns)
        
        with ThreadPoolExecutor() as executor:
            queries = [f'SELECT * FROM {table} WHERE tagid IN ({ids_str}) and t_stamp >= {unix_ms}' 
                      for table in tables]
            results = list(executor.map(self.execute_query, queries))
        
        combined_df = results[0]
        for df in results[1:]:
            combined_df = combined_df.union(df)
            
        return combined_df
    
    def upload_file_to_s3(self, file_path: str, bucket: str, object_name: str = None) -> bool:
        """Upload file to S3 bucket"""
        object_name = object_name if object_name else os.path.basename(file_path)
        s3_client = boto3.client('s3')
        
        try:
            s3_client.upload_file(file_path, bucket, object_name)
            logging.info(f"Uploaded {file_path} to {bucket}/{object_name}")
            return True
        except boto3.exceptions.S3UploadFailedError as e:
            logging.error(f"Upload failed for {file_path} to {bucket}/{object_name}: {str(e)}")
            return False
    
    def download_s3_file(self, bucket: str, key: str, local_path: str) -> bool:
        """Download file from S3 to local path"""
        s3_client = boto3.client('s3')
        
        try:
            with open(local_path, 'wb') as f:
                s3_client.download_fileobj(bucket, key, f)
            logging.info(f"Downloaded {key} to {local_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to download {key} from {bucket}: {str(e)}")
            return False
    
    def find_min_within_one_month(self, timestamps: Dict[str, datetime.datetime]) -> Tuple[Optional[str], Optional[datetime.datetime]]:
        """Find the minimum timestamp within the last month"""
        one_month_ago = datetime.datetime.now() - datetime.timedelta(days=30)
        timestamps_copy = timestamps.copy()
        
        while timestamps_copy:
            min_key, min_date = min(timestamps_copy.items(), key=lambda x: x[1])
            if min_date >= one_month_ago:
                return min_key, min_date
            timestamps_copy.pop(min_key)
            
        return None, None
    
    def process_data(self) -> None:
        """Main data processing workflow"""
        file_path = self.config['local_file_path']
        if not os.path.exists(file_path):
            os.makedirs(file_path)
            logging.info(f"Directory created: {file_path}")
        else:
            logging.info(f"Directory already exists: {file_path}")
        
        last_dates = {}
        actual_data = {}
        
        for filename, var_name in self.config['files'].items():
            try:
                df = self.load_data(os.path.join(file_path, filename), 'parquet')
                max_date = df.agg(F.max('datetime')).collect()[0][0]
                last_dates[var_name] = max_date
                actual_data[filename] = df
            except Exception as e:
                logging.warning(f"Local load failed for {filename}, trying S3: {str(e)}")
                try:
                    s3_path = f"s3://{self.config['bucket_name']}/{self.config['directory_s3']}{filename}"
                    df = self.spark.read.parquet(s3_path)
                    max_date = df.agg(F.max('datetime')).collect()[0][0]
                    last_dates[var_name] = max_date
                    actual_data[filename] = df
                except Exception as e:
                    logging.error(f'No file found on S3 or local for {filename}: {str(e)}')
        
        min_key, min_date = self.find_min_within_one_month(last_dates)
        
        if min_key:
            yr, mon, day = min_date.year, min_date.month, min_date.day
        else:
            yr = self.config.get('default_year', 2024)
            mon = self.config.get('default_month', 8)
            day = self.config.get('default_day', 1)
        
        date_object = datetime.datetime.strptime(f"{yr}-{mon}-{day}", '%Y-%m-%d')
        unix_ms = int(date_object.timestamp() * 1000)
        
        inspector = inspect(self.engine)
        tags = self.spark.createDataFrame(pd.read_sql('SELECT * FROM your_tags_table', self.engine))
        
        tables_data = inspector.get_table_names(schema='public')
        tables = self.filter_tables(tables_data, year=yr, start_month=mon)
        
        final_result = self.get_data_all(tables, tags, self.config['patterns'], unix_ms)
        
        tags_pd = tags.toPandas()
        final_result = final_result.join(
            self.spark.createDataFrame(tags_pd[['id', 'tagpath']]),
            final_result['tagid'] == col('id'),
            'left'
        ).drop('id')
        
        unique_tagpaths = final_result.select('tagpath').distinct().collect()
        
        for row in unique_tagpaths:
            tagpath = row['tagpath']
            data = final_result.filter(col('tagpath') == tagpath)
            data = data.filter(col('dataintegrity') != 0)
            
            if data.count() == 0:
                continue
                
            data = data.withColumn(
                't_stamp',
                F.from_unixtime(col('t_stamp') / 1000).cast(TimestampType())
            )
            
            col_name = tagpath.replace('/', '_')
            data = data.drop('dataintegrity', 'tagpath', 'tagid')
            
            value_col = [c for c in data.columns if c not in ['t_stamp', 'tagid']][0]
            
            data = data.withColumnRenamed('t_stamp', 'datetime') \
                      .withColumnRenamed(value_col, col_name) \
                      .select('datetime', col_name)
            
            existing_data = actual_data.get(f'{col_name}.parquet', None)
            if existing_data:
                combined_data = existing_data.union(data)
            else:
                combined_data = data
                
            combined_data = combined_data.dropDuplicates(['datetime']) \
                                       .sort('datetime')
            
            output_path = f"s3://{self.config['bucket_name']}/{self.config['directory_s3']}{col_name}.parquet"
            combined_data.write.mode('overwrite').parquet(output_path)
            logging.info(f"Data has been written: {output_path}")

def load_config(config_path: str) -> Dict:
    """Load configuration from JSON file"""
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config

def main():
    """Main execution function"""
    try:
        config = load_config('config/config.json')
        processor = DataProcessor(config)
        processor.process_data()
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
