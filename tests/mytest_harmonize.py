import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from netrias_client import NetriasClient, NetriasClientError
from netrias_client._config import Environment


load_dotenv()
client = NetriasClient(api_key=os.environ["NETRIAS_API_KEY"], environment=Environment.STAGING)
# Optional: tune timeout, logging
client.configure(
   log_level="INFO",
   log_directory=Path("./logs"),
)

source = Path("raw_test_files/sampled_2019-07-25-clin.csv")

# Step 1: discover column → CDE mappings
manifest = client.discover_mapping_from_tabular(
      source_path=source,
      target_schema="ccdi",      
      target_version="1",
      sample_limit=5000,
      top_k=3,
      confidence_threshold=0.5,
      generate_raw_overlap_report=True
)
print("Discovered manifest:")
for col_key, mapping in manifest["column_mappings"].items():
      print(f"  {col_key} ({mapping['column_name']}) → {mapping['cde_key']}")


# Step 2: harmonize using that manifest
result = client.harmonize(
      source_path=source,
      manifest=manifest,
      data_commons_key="ccdi",                        
      output_path=Path("output/harmonized.csv"),
      manifest_output_path=Path("output/manifest.json"),
)
