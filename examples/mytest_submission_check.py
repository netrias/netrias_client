import os
from pathlib import Path
from dotenv import load_dotenv
from netrias_client import NetriasClient
from netrias_client._config import Environment



_ = load_dotenv()
client = NetriasClient(api_key=os.environ["NETRIAS_API_KEY"], environment=Environment.STAGING)
# remove 2nd argument to use production environment
client.configure(
   log_level="INFO",
   log_directory=Path("./logs"),
)

EXAMPLE_DIR = Path(__file__).resolve().parent

source = EXAMPLE_DIR / "data" / "sampled_cds_single_sheet.csv"

# Step 1: discover column → CDE mappings
manifest = client.discover_mapping_from_tabular(
      source_path=source,
      target_schema="gc",
      external_version_number="11.0.4",
      sample_limit=5000,
      top_k=3,
      confidence_threshold=0.6,
)


# Step 2: harmonize using that manifest
result = client.harmonize(
      source_path=source,
      manifest=manifest,
      target_schema="gc",
      external_version_number="11.0.4",
      output_path=EXAMPLE_DIR / "output" / "harmonized.csv",
      manifest_output_path=EXAMPLE_DIR / "output" / "manifest.json",
)


# Step 3: Node Discovery: suggest node(s) for the harmonized CSV
recommendations = client.suggest_node(
      harmonized_csv_path=EXAMPLE_DIR / "output" / "harmonized.csv",
      target_schema="gc",
      data_model_outputs_root=EXAMPLE_DIR / "data",
      output_path=EXAMPLE_DIR / "output" / "suggested_nodes.json",
)


# Step 4: Chunk and Validate the harmonized CSV
result = client.chunk_and_validate(
    source_path=source,
    harmonized_csv_path=EXAMPLE_DIR / "output" / "harmonized.csv",
    node_recommendations_path=EXAMPLE_DIR / "output" / "suggested_nodes.json",
    target_schema="gc",
    data_model_outputs_root=EXAMPLE_DIR / "data",
    output_dir=EXAMPLE_DIR / "output",
)
