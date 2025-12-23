import great_expectations as gx

context = gx.get_context()

# ชี้ไปที่ Datasource ที่ถูกต้อง
ds = context.datasources["brazilian_ecommerce_postgres"]

# -----------------------------
# 1) Mart Sales Expectations
# -----------------------------
suite_name_sales = "mart_sales_suite"
context.add_or_update_expectation_suite(suite_name_sales)

asset_sales = ds.get_asset("fct_sales") 
validator_sales = context.get_validator(
    batch_request=asset_sales.build_batch_request(),
    expectation_suite_name=suite_name_sales,
)

# Expectations for fct_sales
validator_sales.expect_column_values_to_not_be_null("order_id")
validator_sales.expect_column_values_to_be_between("price", min_value=0)
validator_sales.expect_column_values_to_not_be_null("order_date_key")
validator_sales.expect_column_values_to_not_be_null("customer_key")
validator_sales.expect_column_values_to_not_be_null("product_key")
validator_sales.expect_column_values_to_not_be_null("seller_key")
validator_sales.save_expectation_suite(discard_failed_expectations=False)
print(f"✅ Created expectation suite: {suite_name_sales}")

# -----------------------------
# 2) Mart Customer Expectations
# -----------------------------
suite_name_customer = "mart_customer_suite"
context.add_or_update_expectation_suite(suite_name_customer)

asset_customer = ds.get_asset("fct_customers") 
validator_customer = context.get_validator(
    batch_request=asset_customer.build_batch_request(),
    expectation_suite_name=suite_name_customer,
)

# Expectations for fct_customers
validator_customer.expect_column_values_to_be_unique("customer_key") # PK
validator_customer.expect_column_values_to_not_be_null("customer_key")
validator_customer.expect_column_values_to_be_between("total_orders", min_value=0)
validator_customer.expect_column_values_to_be_between("total_spent", min_value=0)
validator_customer.save_expectation_suite(discard_failed_expectations=False)
print(f"✅ Created expectation suite: {suite_name_customer}")


# -----------------------------
# 3) Mart Product Expectations
# -----------------------------
suite_name_product = "mart_product_suite"
context.add_or_update_expectation_suite(suite_name_product)

asset_product = ds.get_asset("fct_product") 
validator_product = context.get_validator(
    batch_request=asset_product.build_batch_request(),
    expectation_suite_name=suite_name_product,
)

# Expectations for fct_product
validator_product.expect_column_values_to_be_unique("product_performance_pk") # PK
validator_product.expect_column_values_to_not_be_null("product_performance_pk")
validator_product.expect_column_values_to_not_be_null("product_key")
validator_product.expect_column_values_to_not_be_null("seller_key")
validator_product.expect_column_values_to_not_be_null("order_date_key")
validator_product.expect_column_values_to_be_between("revenue", min_value=0)
validator_product.expect_column_values_to_be_between("units_sold", min_value=1, max_value=1)
validator_product.save_expectation_suite(discard_failed_expectations=False)
print(f"✅ Created expectation suite: {suite_name_product}")