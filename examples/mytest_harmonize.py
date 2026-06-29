import os
from pathlib import Path
from dotenv import load_dotenv
from netrias_client import NetriasClient
from netrias_client._config import Environment


load_dotenv()
client = NetriasClient(api_key=os.environ["NETRIAS_API_KEY"])
# Optional: tune timeout, logging
client.configure(
   log_level="INFO",
   log_directory=Path("./logs"),
)

source = Path("examples/data/demo_overlap.csv")

# Step 1: discover column → CDE mappings
manifest = client.discover_mapping_from_tabular(
      source_path=source,
      target_schema="ccdi",
      external_version_number="1",
      sample_limit=5000,
      top_k=3,
      confidence_threshold=0.6,
      generate_raw_overlap_report=True,               # optional, defaults to False
      overlap_report_output_dir=Path("output"),       # optional, defaults to "output"
)
print("Discovered manifest:")
for col_key, mapping in manifest["column_mappings"].items():
      print(f"  {col_key} ({mapping['column_name']}) → {mapping['cde_key']}")


# Step 2: harmonize using that manifest
result = client.harmonize(
      source_path=source,
      manifest=manifest,
      data_commons_key="ccdi",
      external_version_number="1",
      output_path=Path("output/harmonized.csv"),
      manifest_output_path=Path("output/manifest.json"),
)
