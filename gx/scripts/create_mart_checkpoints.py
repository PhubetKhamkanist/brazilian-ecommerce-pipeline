import great_expectations as gx

context = gx.get_context()

# ✅ แก้ไข: ชี้ไปที่ Datasource ที่ถูกต้อง
ds = context.datasources["brazilian_ecommerce_postgres"]

# -----------------------------
# 1) Checkpoint สำหรับ Mart Sales
# -----------------------------
# ✅ แก้ไข: ดึง Asset 'fct_sales'
asset_sales = ds.get_asset("fct_sales")
context.add_or_update_checkpoint(
    name='mart_sales_checkpoint',
    validations=[
        {
            "batch_request": asset_sales.build_batch_request(),
            "expectation_suite_name": "mart_sales_suite",
        }
    ],
)
print("✅ Created checkpoint: mart_sales_checkpoint")

# -----------------------------
# 2) Checkpoint สำหรับ Mart Customer
# -----------------------------
# ✅ แก้ไข: ดึง Asset 'fct_customers'
asset_customer = ds.get_asset("fct_customers")
context.add_or_update_checkpoint(
    name='mart_customer_checkpoint', 
    validations=[
        {
            "batch_request": asset_customer.build_batch_request(),
            "expectation_suite_name": "mart_customer_suite",
        }
    ],
)
print("✅ Created checkpoint: mart_customer_checkpoint")

# -----------------------------
# 3) Checkpoint สำหรับ Mart Product
# -----------------------------
# ✅ แก้ไข: ดึง Asset 'fct_product'
asset_product = ds.get_asset("fct_product")
context.add_or_update_checkpoint(
    name='mart_product_checkpoint', 
    validations=[
        {
            "batch_request": asset_product.build_batch_request(),
            "expectation_suite_name": "mart_product_suite",
        }
    ],
)
print("✅ Created checkpoint: mart_product_checkpoint")