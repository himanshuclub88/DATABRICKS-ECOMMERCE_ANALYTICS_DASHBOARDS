# E-Commerce Analytics ETL Pipeline
# PySpark code for processing e-commerce data and creating analytics tables

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ECommerceETL")

def create_spark_session():
    """Initialize Spark Session"""
    return SparkSession.builder \
        .appName("ECommerceAnalytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def extract_data(spark, volume_path):
    """Extract data from Unity Catalog Volume"""
    logger.info("Starting data extraction...")

    # Define schema for better performance
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("region", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_percent", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    # Read raw data
    orders_raw = spark.read \
        .option("header", "true") \
        .schema(orders_schema) \
        .csv(f"{volume_path}/ecommerce_orders.csv")

    logger.info(f"Extracted {orders_raw.count()} orders from source")
    return orders_raw

def transform_data(orders_df):
    """Transform and clean the data"""
    logger.info("Starting data transformation...")

    # Data cleaning and transformations
    orders_clean = orders_df \
        .filter(col("status") == "Completed") \
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
        .withColumn("year", year(col("order_date"))) \
        .withColumn("month", month(col("order_date"))) \
        .withColumn("quarter", quarter(col("order_date"))) \
        .withColumn("day_of_week", dayofweek(col("order_date"))) \
        .withColumn("month_name", date_format(col("order_date"), "MMMM")) \
        .withColumn("revenue", col("total_amount")) \
        .withColumn("profit_margin", 
                   when(col("category") == "Electronics", col("total_amount") * 0.15)
                   .when(col("category") == "Fashion", col("total_amount") * 0.25)
                   .when(col("category") == "Home", col("total_amount") * 0.20)
                   .when(col("category") == "Sports", col("total_amount") * 0.22)
                   .otherwise(col("total_amount") * 0.30)) \
        .withColumn("customer_tier", 
                   when(col("total_amount") >= 500, "Premium")
                   .when(col("total_amount") >= 100, "Standard")
                   .otherwise("Basic")) \
        .withColumn("load_timestamp", current_timestamp())

    logger.info(f"Transformed data: {orders_clean.count()} clean orders")
    return orders_clean

def create_analytics_tables(spark, orders_df, catalog, schema):
    """Create various analytics tables for dashboard"""

    logger.info("Creating analytics tables...")

    # 1. Daily Sales Summary
    daily_sales = orders_df \
        .groupBy("order_date", "year", "month", "quarter") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            sum("quantity").alias("total_quantity"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy("order_date")

    daily_sales.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.daily_sales_summary")

    logger.info("Created daily_sales_summary table")

    # 2. Category Performance
    category_performance = orders_df \
        .groupBy("category") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            sum("profit_margin").alias("total_profit"),
            avg("total_amount").alias("avg_order_value"),
            sum("quantity").alias("total_quantity"),
            countDistinct("customer_id").alias("unique_customers"),
            avg("discount_percent").alias("avg_discount")
        ) \
        .withColumn("profit_margin_pct", 
                   round((col("total_profit") / col("total_revenue")) * 100, 2)) \
        .orderBy(desc("total_revenue"))

    category_performance.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.category_performance")

    logger.info("Created category_performance table")

    # 3. Regional Analysis
    regional_analysis = orders_df \
        .groupBy("region") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("product_name").alias("unique_products")
        ) \
        .withColumn("revenue_percentage", 
                   round((col("total_revenue") / sum("total_revenue").over(Window.partitionBy())) * 100, 2)) \
        .orderBy(desc("total_revenue"))

    regional_analysis.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.regional_analysis")

    logger.info("Created regional_analysis table")

    # 4. Channel Performance
    channel_performance = orders_df \
        .groupBy("channel") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy(desc("total_revenue"))

    channel_performance.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.channel_performance")

    logger.info("Created channel_performance table")

    # 5. Customer Segmentation
    customer_analysis = orders_df \
        .groupBy("customer_id", "customer_tier") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_order_value"),
            first("order_date").alias("first_order"),
            last("order_date").alias("last_order")
        ) \
        .withColumn("customer_lifetime_days", 
                   datediff(col("last_order"), col("first_order"))) \
        .withColumn("order_frequency", 
                   when(col("customer_lifetime_days") > 0, 
                        col("total_orders") / (col("customer_lifetime_days") / 30))
                   .otherwise(col("total_orders")))

    customer_analysis.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.customer_analysis")

    logger.info("Created customer_analysis table")

    # 6. Product Performance
    product_performance = orders_df \
        .groupBy("product_name", "category") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            sum("quantity").alias("total_sold"),
            avg("unit_price").alias("avg_price"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .withColumn("revenue_rank", 
                   row_number().over(Window.orderBy(desc("total_revenue")))) \
        .orderBy("revenue_rank")

    product_performance.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.product_performance")

    logger.info("Created product_performance table")

    # 7. Monthly Trends
    monthly_trends = orders_df \
        .groupBy("year", "month", "month_name") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .withColumn("month_year", concat(col("month_name"), lit(" "), col("year"))) \
        .orderBy("year", "month")

    monthly_trends.write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.monthly_trends")

    logger.info("Created monthly_trends table")

def main_etl_pipeline():
    """Main ETL pipeline execution"""

    # Configuration
    catalog_name = "workspace"
    schema_name = "ecommerce_analytics"
    volume_path = "/Volumes/workspace/development/ecommerce_data"

    try:
        # Initialize Spark
        spark = create_spark_session()
        logger.info("Spark session initialized")

        # Create schema if not exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

        # ETL Steps
        logger.info("=== STARTING E-COMMERCE ETL PIPELINE ===")

        # Extract
        orders_raw = extract_data(spark, volume_path)

        # Transform
        orders_clean = transform_data(orders_raw)

        # Load main table
        orders_clean.write \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{catalog_name}.{schema_name}.orders_fact")

        logger.info("Created main orders_fact table")

        # Create analytics tables
        create_analytics_tables(spark, orders_clean, catalog_name, schema_name)

        # Show summary
        logger.info("=== ETL PIPELINE COMPLETED SUCCESSFULLY ===")
        logger.info(f"Tables created in {catalog_name}.{schema_name}:")
        logger.info("1. orders_fact - Main fact table")
        logger.info("2. daily_sales_summary - Daily aggregations")
        logger.info("3. category_performance - Category metrics")
        logger.info("4. regional_analysis - Regional breakdown")
        logger.info("5. channel_performance - Channel analytics")
        logger.info("6. customer_analysis - Customer segmentation")
        logger.info("7. product_performance - Product rankings")
        logger.info("8. monthly_trends - Time series analysis")

        # Display sample results
        print("\nSample Category Performance:")
        spark.table(f"{catalog_name}.{schema_name}.category_performance").show()

        print("\nSample Regional Analysis:")
        spark.table(f"{catalog_name}.{schema_name}.regional_analysis").show()

        return True

    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
        raise

    finally:
        if 'spark' in locals():
            spark.stop()

# Execute the pipeline
if __name__ == "__main__":
    main_etl_pipeline()
