import great_expectations as gx

# โหลด GX context
context = gx.get_context()

# 1. แก้ไข Connection String ให้ตรงกับฐานข้อมูลของคุณ
ds = context.sources.add_or_update_sql(
    name="brazilian_ecommerce_postgres",  # ตั้งชื่อ datasource ให้สื่อความหมาย
    connection_string="postgresql+psycopg2://dpuser:dppass@dp_postgres:5432/brazilian_ecommerce"
)

# 2. เพิ่ม Assets (ตาราง) ทั้งหมดที่เรามีใน schema 'staging'
staging_table_names = [
    "olist_customers_dataset", "olist_geolocation_dataset",
    "olist_order_items_dataset", "olist_order_payments_dataset",
    "olist_order_reviews_dataset", "olist_orders_dataset",
    "olist_products_dataset", "olist_sellers_dataset",
    "product_category_name_translation"
]

for table in staging_table_names:
    ds.add_table_asset(
        name=table,
        table_name=table,
        schema_name="staging"
    )
print(f"✅ Added {len(staging_table_names)} assets from 'staging' schema.")


# 3. 🆕 เพิ่ม Assets (ตาราง) สำหรับ Marts (สำคัญมาก!)
# --------------------------------------------------
ds.add_table_asset(
    name="fct_sales",
    table_name="fct_sales",
    schema_name="mart_sales" # schema ที่ dbt สร้าง
)
print("✅ Added mart asset: fct_sales (from schema: mart_sales)")

ds.add_table_asset(
    name="fct_customers",
    table_name="fct_customers",
    schema_name="mart_customers" # schema ที่ dbt สร้าง
)
print("✅ Added mart asset: fct_customers (from schema: customers)")

ds.add_table_asset(
    name="fct_product",
    table_name="fct_product",
    schema_name="mart_products" # schema ที่ dbt สร้าง
)
print("✅ Added mart asset: fct_product (from schema: products)")

print(f"\n✅ Datasource 'brazilian_ecommerce_postgres' is now ready with Staging and Mart assets.")