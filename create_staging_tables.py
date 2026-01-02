from db_config import get_connection

def run_sql(sql):
    conn = get_connection()
    cursor = conn.cursor()

    for statement in sql.split(";"):
        stmt = statement.strip()
        if stmt:
            cursor.execute(stmt)

    conn.commit()
    cursor.close()
    conn.close()


def create_tables():

    print("Creating staging tables...\n")

    tables = [

        # ------------------ Orders ------------------
        """
        IF OBJECT_ID('dbo.stg_orders', 'U') IS NOT NULL
            DROP TABLE dbo.stg_orders;
            
        CREATE TABLE dbo.stg_orders (
            order_id CHAR(32) PRIMARY KEY,
            customer_id CHAR(32) NOT NULL,
            order_status NVARCHAR(20),
            order_purchase_timestamp DATETIME2,
            order_approved_at DATETIME2,
            order_delivered_carrier_date DATETIME2,
            order_delivered_customer_date DATETIME2,
            order_estimated_delivery_date DATETIME2
        );
        """,

        # ------------------ Order Items  ------------------
        """
        IF OBJECT_ID('dbo.stg_order_items', 'U') IS NOT NULL
            DROP TABLE dbo.stg_order_items;
            
        CREATE TABLE dbo.stg_order_items (
            order_id CHAR(32) NOT NULL,
            order_item_id SMALLINT NOT NULL,
            product_id CHAR(32) NOT NULL,
            seller_id CHAR(32) NOT NULL,
            shipping_limit_date DATETIME2,    
            price DECIMAL(10,2),
            freight_value DECIMAL(10,2),
            CONSTRAINT PK_stg_order_items PRIMARY KEY(order_id, order_item_id)
        );
        """,

        # ------------------ Payments  ------------------
        """
        IF OBJECT_ID('dbo.stg_order_payments', 'U') IS NOT NULL
            DROP TABLE dbo.stg_order_payments;
            
        CREATE TABLE dbo.stg_order_payments (
            order_id CHAR(32) NOT NULL,
            payment_sequential SMALLINT NOT NULL,
            payment_type NVARCHAR(20),
            payment_installments SMALLINT,
            payment_value DECIMAL(10,2),
            CONSTRAINT PK_stg_order_payments PRIMARY KEY(order_id, payment_sequential)
        );
        """,

        # ------------------ Reviews ------------------
        """
        IF OBJECT_ID('dbo.stg_order_reviews', 'U') IS NOT NULL
            DROP TABLE dbo.stg_order_reviews;
            
        CREATE TABLE dbo.stg_order_reviews (
            review_id CHAR(32),
            order_id CHAR(32),
            review_score TINYINT,
            review_comment_title NVARCHAR(50),
            review_comment_message NVARCHAR(MAX),
            review_creation_date DATETIME2,
            review_answer_timestamp DATETIME2,
            CONSTRAINT PK_stg_order_reviews PRIMARY KEY (review_id, order_id)
        );
        """,

        # ------------------ Products ------------------
        """
        IF OBJECT_ID('dbo.stg_products', 'U') IS NOT NULL
            DROP TABLE dbo.stg_products;
            
        CREATE TABLE dbo.stg_products (
            product_id CHAR(32) PRIMARY KEY,
            product_category_name VARCHAR(50),
            product_name_length SMALLINT,
            product_description_length SMALLINT,
            product_photos_qty TINYINT,
            product_weight_g INT,
            product_length_cm SMALLINT,
            product_height_cm SMALLINT,
            product_width_cm SMALLINT
        );
        """,

        # ------------------ Customers ------------------
        """
        IF OBJECT_ID('dbo.stg_customers', 'U') IS NOT NULL
            DROP TABLE dbo.stg_customers;
            
        CREATE TABLE dbo.stg_customers (
            customer_id CHAR(32) PRIMARY KEY,
            customer_unique_id CHAR(32),
            customer_zip_code_prefix INT,
            customer_city NVARCHAR(100),
            customer_state CHAR(2)
        );
        """,

        # ------------------ Sellers ------------------
        """
        IF OBJECT_ID('dbo.stg_sellers', 'U') IS NOT NULL
            DROP TABLE dbo.stg_sellers;
            
        CREATE TABLE dbo.stg_sellers (
            seller_id CHAR(32) PRIMARY KEY,
            seller_zip_code_prefix INT,
            seller_city NVARCHAR(100),
            seller_state CHAR(2)
        );
        """,

        # ------------------ Geolocation ------------------
        """
        IF OBJECT_ID('dbo.stg_geolocation', 'U') IS NOT NULL
            DROP TABLE dbo.stg_geolocation;
            
        CREATE TABLE dbo.stg_geolocation (
            geolocation_zip_code_prefix INT NOT NULL,
            geolocation_lat DECIMAL(9,6) NOT NULL,
            geolocation_lng DECIMAL(9,6) NOT NULL,
            geolocation_city NVARCHAR(100),
            geolocation_state CHAR(2),
        );
        """,
        
        #------------------ product category translation----------------
        """
        IF OBJECT_ID('dbo.stg_product_category_translation', 'U') IS NOT NULL
            DROP TABLE dbo.stg_product_category_translation;
            
        CREATE TABLE dbo.stg_product_category_translation(
        product_category_name          NVARCHAR(50)  PRIMARY KEY,
        product_category_name_english  NVARCHAR(100)
        );
        """
    ]

    for sql in tables:
        run_sql(sql)

    print("All staging tables created successfully!\n")


