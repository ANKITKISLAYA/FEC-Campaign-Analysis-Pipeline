from pyspark.sql import SparkSession

def spark_session(config_yml, app_name= None):
    """
    Create and return a Spark session.
    
    Returns:
        SparkSession: A Spark session object.
    """
    # If app_name is not provided, use the default from the config
    if app_name is None:
        app_name = config_yml['spark']['default_app_name']
        
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", config_yml['spark']['shuffle_partitions']) \
        .config("spark.executor.memory", config_yml['spark']['executor_memory']) \
        .config("spark.driver.memory", config_yml['spark']['driver_memory']) \
        .config("spark.executor.cores", config_yml['spark']['executor_cores']) \
        .config("spark.dynamicAllocation.enabled", config_yml['spark']['dynamic_allocation']) \
        .getOrCreate()
    return spark

if __name__ == "__main__":
    spark_session()