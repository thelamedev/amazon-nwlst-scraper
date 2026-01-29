import polars as pl

df = pl.read_json("batch_results_v2.json")

total_count = df.count().row(0)[0]
success_count = df.filter(pl.col("status") == "ok").count().row(0)[0]
error_count = df.filter(pl.col("status") == "error").count().row(0)[0]

success_rate = success_count / total_count
error_rate = error_count / total_count

print(f"⭕️ {total_count} URLs")
print(f"✅ {success_count} URLs | {success_rate:.2%} success rate")
print(f"❌ {error_count} URLs | {error_rate:.2%} error rate")
