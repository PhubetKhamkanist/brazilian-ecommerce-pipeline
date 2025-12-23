import great_expectations as gx

context = gx.get_context()

# 1. ดึง datasource/asset ที่เกี่ยวข้อง
ds = context.datasources["brazilian_ecommerce_postgres"]
asset = ds.get_asset("olist_orders_dataset")

# 2. สร้าง Checkpoint โดยใช้ชื่อที่ถูกต้อง
checkpoint_name = 'brazilian_ecommerce_staging_checkpoint'
checkpoint = context.add_or_update_checkpoint(
    name=checkpoint_name,
    validations=[
        {
            "batch_request": asset.build_batch_request(),
            "expectation_suite_name": "staging_olist_orders_checks", # ใช้ชื่อ suite จากขั้นตอนที่ 2
        }
    ],
)

print(f"✅ Checkpoint '{checkpoint_name}' is ready.")

# 3. รันทดสอบทันทีเพื่อยืนยันว่าทำงานได้
print("\nRunning checkpoint to verify setup...")
result = context.run_checkpoint(checkpoint_name)

print(f"\nCheckpoint run result: {'Success' if result['success'] else 'Failed'}")