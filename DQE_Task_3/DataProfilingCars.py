import os
import pandas as pd
from ydata_profiling import ProfileReport
# Read parquet
df = pd.read_parquet("car_prices.parquet")

# Save as CSV
df.to_csv("car_prices.csv", index=False)

# Compare file sizes
parquet_size = os.path.getsize("car_prices.parquet") / (1024 * 1024)  # MB
csv_size = os.path.getsize("car_prices.csv") / (1024 * 1024)          # MB

print(f"Parquet file size: {parquet_size:.2f} MB")
print(f"CSV file size: {csv_size:.2f} MB")
# Create the profiling report
profile = ProfileReport(df, title="Car Prices Profiling Report", explorative=True)


# Save report as HTML
profile.to_file("car_prices_profiling.html")