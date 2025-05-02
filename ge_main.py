import great_expectations as gx
from pyspark.sql import SparkSession
import yaml
import datetime

# ---------- CONFIGURATION ----------

DATA_SOURCE_NAME = "SPARK"
DATA_ASSET_NAME = "CSV_FILE"
BATCH_DEFINITION_NAME = f"ge_{datetime.datetime.today().strftime('%d%b%Y').upper()}"
SUITE_NAME = "BASIC_CHECK_SUITE"
VALIDATION_DEFINITION_NAME = "preliminary_validations"
CSV_FILE_PATH = "data/sample_data.csv"
EXPECTATIONS_YAML_PATH = "expectations/csv_suite.yaml"

# ---------- UTILITY FUNCTIONS ----------

def load_expectations_from_yaml(file_path):
    """
    Load expectation definitions from a YAML file.
    """
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    return data["expectations"]

# ---------- INITIALIZE SPARK ----------

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark_df = (
    spark.read
    .option('header', 'true')
    .option('inferSchema', 'true')
    .csv(CSV_FILE_PATH)
)

# ---------- INITIALIZE GREAT EXPECTATIONS ----------

context = gx.get_context()
#
# Add Spark data source if not already present
if DATA_SOURCE_NAME not in context.data_sources.all().keys():
    context.data_sources.add_spark(name=DATA_SOURCE_NAME)

# Add or get DataFrame asset
data_source = context.data_sources.get(DATA_SOURCE_NAME)
if DATA_ASSET_NAME not in data_source.assets:
    data_source.add_dataframe_asset(name=DATA_ASSET_NAME)
data_asset = data_source.get_asset(DATA_ASSET_NAME)

# ---------- BATCH DEFINITION ----------

# Register the whole DataFrame as a batch
if BATCH_DEFINITION_NAME not in data_asset.batch_definitions:
    data_asset.add_batch_definition_whole_dataframe(BATCH_DEFINITION_NAME)

batch_definition = data_asset.get_batch_definition(BATCH_DEFINITION_NAME)

# ---------- EXPECTATION SUITE ----------

# Create or retrieve expectation suite
if not any(d.get("name") == SUITE_NAME for d in context.suites.all()):
    suite = gx.ExpectationSuite(name=SUITE_NAME)
    context.suites.add(suite)
else:
    suite = context.suites.get(SUITE_NAME)

# Load expectations from YAML and add to suite
expectations = load_expectations_from_yaml(EXPECTATIONS_YAML_PATH)
for exp in expectations:
    expectation_type = exp["expectation_type"]
    kwargs = exp.get("kwargs", {})
    expectation_class = getattr(gx.expectations, expectation_type)
    suite.add_expectation(expectation_class(**kwargs))

# ---------- VALIDATION ----------

# Create or retrieve validation definition
if VALIDATION_DEFINITION_NAME not in context.validation_definitions.all():
    validation_definition = gx.ValidationDefinition(
        data=batch_definition, suite=suite, name=VALIDATION_DEFINITION_NAME
    )
    context.validation_definitions.add(validation_definition)
else:
    validation_definition = context.validation_definitions.get(VALIDATION_DEFINITION_NAME)

# Run validation
batch_parameters = {"dataframe": spark_df}
validation_results = validation_definition.run(batch_parameters=batch_parameters)

# ---------- RESULTS ----------

for key, value in validation_results.items():
    print(f"Key: {key}, value: {value}")
    print("====================")
