# Great Expectations Spark CSV Validation

This project demonstrates how to use the Great Expectations library to validate a CSV file loaded as a Spark DataFrame.

Great Expectations is a powerful open-source Python-based data validation and quality framework. It allows you to:
✅ Define “expectations” (rules) for your data
✅ Validate your data against these expectations
✅ Generate clear, human-readable reports of data quality

In this project, we:
	•	Load expectations from a YAML file
	•	Register a Spark DataFrame as a data asset
	•	Define batch and validation configurations
	•	Run validations and print the results

📦 Requirements 
    Python 3.x
	•	PySpark
	•	Great Expectations (great_expectations package)
	•	PyYAML (yaml package)



📂 Project Structure

/data
    sample_data.csv
/expectations
    csv_suite.yaml
main.py
README.md

⚙️ Code Overview

The script (main.py) does the following:
	1.	Setup Spark Session
Initializes a local Spark session and reads the CSV file as a DataFrame.
	2.	Initialize Great Expectations Context
Adds the Spark data source and registers the DataFrame as a data asset.
	3.	Load Expectations
Reads a list of expectations from a YAML file and adds them to the expectation suite.
	4.	Define Batch and Validation
Configures batch and validation definitions.
	5.	Run Validation
Executes the validation and prints results to the console.

📄 Example YAML Expectations

The expectations YAML file (expectations/csv_suite.yaml) should look like:

expectations:
  - expectation_type: ExpectColumnValuesToNotBeNull
    kwargs:
      column: "id"
  - expectation_type: ExpectColumnValuesToBeUnique
    kwargs:
      column: "id"

🚀 How to Run

1️⃣ Install dependencies:

pip install great_expectations pyspark pyyaml

2️⃣ Place your CSV file in data/sample_data.csv and your YAML file in expectations/csv_suite.yaml.

3️⃣ Run the script:

python main.py

📝 Output

The script will print the validation results like:

Key: success, value: True
====================
Key: statistics, value: {...}
====================

