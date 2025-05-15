import os
import json
import argparse
import pandas as pd
import re
import sys
import csv
import chardet
import psycopg2
from configuration import log, DATA_CONTRACT_DEFAULT_PATH, PARSED_DATA_CONTRACT_DEFAULT_PATH, TEMPLATE, CODES_DIR
from lib.utils import findLoc, listToComma

def read_input_args():
    try:
        log.info('Data Contract File Path Function: Started')
        parser = argparse.ArgumentParser(description='The Arguments --file, --dir and postgres creds are required')
        parser.add_argument('--file', '-f', help="Data Contract File Name", type=str, required=True)
        parser.add_argument('--dir', '-d', help="Data Contract Directory Path", type=str, default=DATA_CONTRACT_DEFAULT_PATH)
        parser.add_argument('--psql_db', help="Postgres Database", required=True)
        parser.add_argument('--psql_user', help="Postgres Username", required=True)
        parser.add_argument('--psql_password', help="Postgres Password", required=True)
        parser.add_argument('--psql_host', help="Postgres Hostname", required=True)
        parser.add_argument('--psql_port', help="Postgres Port", required=True)
        args = parser.parse_args()
        return args
    except Exception as e:
        log.error("Error reading input arguments", exc_info=True)
        sys.exit(1)


def connect_to_postgres(psql_db, psql_user, psql_password, psql_host, psql_port):
    try:
        connection = psycopg2.connect(
            dbname=psql_db,
            user=psql_user,
            password=psql_password,
            host=psql_host,
            port=psql_port
        )
        cursor = connection.cursor()
        log.info("Successfully connected to the database")
        return connection, cursor
    except Exception as error:
        log.error(f"Error connecting to PostgreSQL: {error}")
        return None, None

def initialize_context():
    return {
        "issues": [],
        "other_issues": [],
        "all_issues": [],
        "template": json.load(TEMPLATE),
        "template_file": TEMPLATE,
        "files": [],
        "dataset_issues": {},
        "cursor": None,
        "connection": None,
        # validation-specific data
        "attribute_issues": [],
        "attribute_classification_issues": [],
        "attribute_nullability_issues": [],
        "attribute_primary_issues": [],
        "attribute_primary_logic_issues": [],
        "attribute_uniqueness_issues": [],
        "category_issues": [],
        "category_issues_merge": [],
        "classification_issues": [],
        "connectivity_desc_issues": [],
        "connectivity_descriptions": [],
        "connectivity_option_issues": [],
        "data_type_issues": [],
        "data_type_size_issues": [],
        "date_format_issues": [],
        "delimiter_issues": [],
        "delimiter_issues_merge": [],
        "delimiter_values": [],
        "delimiters": [],
        "encoding_issues": [],
        "entity_issues": [],
        "entity_issues_merge": [],
        "entities": [],
        "format_issues": [],
        "frequency_issues": [],
        "ingestion_issues": [],
        "ingestion_type_values": [],
        "language_issues": [],
        "primary_keys": [],
        "service_issues": [],
        "service_issues_merge": [],
        "service_values": [],
        "services": [],
        "split_logic_issues": []
    }

def process_file(file, data_contract_path, context):
    try:
        log.info(f"Processing file: {file}")
        file_path = os.path.join(data_contract_path, file)
        dfs = parse_data_contract(file_path, context)
        validate_headers(dfs, file, context)
        validate_datasets(dfs, file, context, data_contract_path)
        write_issues(file, context)
    except Exception as e:
        log.error("Exception occurred in process_file", exc_info=True)
        sys.exit(1)

def parse_data_contract(path, context):
    log.info(f"Reading data contract from {path}")
    return pd.read_excel(path, sheet_name="Metadata Template", header=2)

def validate_headers(dfs, file, context):
    expected_columns = context['template'].get('headers', [])
    actual_columns = list(dfs.columns)
    missing_columns = [col for col in expected_columns if col not in actual_columns]

    if missing_columns:
        issue = {
            "type": "ERROR",
            "issueValue": "Missing headers",
            "expectedValue": f"All expected headers: {expected_columns}",
            "actualValue": f"Found headers: {actual_columns}",
            "location": f"File: {file}",
            "issueDesc": f"Missing columns: {missing_columns}"
        }
        context['issues'].append(issue)
        log.error("Missing headers in file", extra=issue)

def parse_datasets(dfs, context):


        for idx, (_, row) in enumerate(dataset_df.iterrows()):

            if not attribute_executed:
                
                
   
            current_count = {"index": idx + 1, "count": count}

            
        processed_datasets.append(dataset_name_clean)

    return dfs, processed_datasets
    
def validate_datasets(dfs, file, context, data_contract_path):

    dfs = dfs.dropna(how='all').ffill()
    dataset = dfs['Dataset Name'].dropna().unique()
    entity_name = dfs['Entity'].dropna().unique()
    attribute_col_index = dfs.columns.get_loc('Attribute')
    classification_col_index = dfs.columns.get_loc('Attribute Classification')
    processed_datasets = []

    index_range = list(dfs.loc[dfs['Dataset Name'] == dataset].index)
    start_index = index_range[0]
    end_index = index_range[-1] + 1
               
    attribute_list = dfs.values[start_index:end_index, attribute_col_index]
    attribute_classification_list = dfs.values[start_index:end_index, classification_col_index]
    
    for idx, row in dfs.iterrows():
    
        count = {"index": idx + 1, "count": len(dfs)}
        validate_frequency(row.get('Frequency'), count, dfs, 'Frequency of Update on Source', idx, context)
        validate_frequency(row.get('Frequency'), count, dfs, 'Frequency of Update to SDP', idx, context)
        validate_format(row.get('Format (MIME)'), count, dfs, idx, context)
        validate_encoding(row.get('Code Page'), count, dfs, idx, context)
        validate_attribute(row.get('Attribute'), count, dfs, idx, context)
        validate_split_logic(row.get('Split Logic'), count, dfs, idx, context)
        validate_ingestion_type(row.get('Ingestion Logic'), count, dfs, idx, context)
        validate_attribute_nullability(row.get('Attribute Nullability'), count, dfs, idx, context)
        validate_attribute_uniqueness(row.get('Attribute Uniqueness'), count, dfs, idx, context)
        validate_language(row.get('Language'), count, dfs, idx, context)
        validate_data_type(row.get('Attribute DataType'), count, dfs, idx, context)
        validate_data_type_size(row.get('Attribute Size'), row.get('Attribute DataType'), count, dfs, idx, context)
        validate_date_format(row.get('Attribute Range of Values'), row.get('Attribute DataType'), count, dfs, idx, context)
        validate_connectivity(row.get('Connectivity Option'), count, dfs, idx, context)
        validate_connectivity(row.get('Description for Connectivity'), count, dfs, idx, context)
        validate_delimiter(row.get('Attribute Delimiter'), row.get('Attribute Delimiter- Other'), count, dfs, idx, context)
        validate_attribute_classification(row.get('Attribute Classification'), count, dfs, idx, context)
        validate_service(row.get('Service'), count, dfs, idx, context)
        validate_entity(row.get('Entity'), count, dfs, idx, context)
        validate_category(row.get('Category'), count, dfs, idx, context)
        validate_data_contract_type(row.get('DataContract Type'), count, dfs, idx, context)
        validate_attribute_primary_key(row.get('Attribute Primary Key'), count, dfs, idx, context)
        validate_dataset_classification(row.get('Data Classification Type'), count, dfs, idx, attribute_classification_list, context)
        
    parse_sample_file(dataset_name, entity_name, attributes, context, data_contract_path)



def parse_sample_file(dataset_name, attributes, context, data_contract_path):
    dataset_file_prefix = entity_name + '_' + dataset_name
    sample_dir = os.path.join(data_contract_path, 'sampleFiles')
    sample_file_issues = []

    if not os.path.isdir(sample_dir):
        return

    sample_files = {}
    for file_name in os.listdir(sample_dir):
        name_parts = tuple(file_name.split('.'))
        sample_files[name_parts[0]] = name_parts[1]

    if dataset_file_prefix not in sample_files:
        issue = {
            "type": "ERROR",
            "issueValue": f'The Sample File Doesn\'t exist for dataset {dataset_file_prefix}',
            "expectedValue": f"The Sample File named {dataset_file_prefix} should exist in the sampleFiles directory",
            "actualValue": "No Sample File Exists",
            "location": f"Sample File {dataset_file_prefix}",
            "issueDesc": "Expected a sample file with matching name"
        }
        sample_file_issues.append(issue)
    else:
        ext = sample_files[dataset_file_prefix]
        file_path = os.path.join(sample_dir, f"{dataset_file_prefix}.{ext}")

        try:
            encoding = predict_encoding(file_path, 5)
            if encoding.lower() not in ['utf-8', 'ascii']:
                issue = {
                    "type": "ERROR",
                    "issueValue": f'The Sample File is not in UTF-8 Format',
                    "expectedValue": f"The Sample File for dataset {dataset_file_prefix} must be in UTF-8 format",
                    "actualValue": f"Format is Sample File is: {encoding.upper()}",
                    "location": f"Sample File {dataset_file_prefix}",
                    "issueDesc": "The Sample File is not in UTF-8 Format"
                }
                sample_file_issues.append(issue)

            if ext in ['csv', 'txt']:
                with open(file_path, 'r', encoding="utf8") as f:
                    dialect = csv.Sniffer().sniff(f.read())
                    delimiter = str(dialect.delimiter)

                df_reader = pd.read_csv(file_path, sep=delimiter, header=[0, 0], dtype='unicode')
                sample_count = len(df_reader.index)
                declared_delimiter = context.get('delimiter_values', [''])[0]

                if declared_delimiter.strip().lower() == 'tab':
                    declared_delimiter = '\t'

                if delimiter != declared_delimiter:
                    issue = {
                        "type": "ERROR",
                        "issueValue": f"Sample File delimiter is {delimiter} but Data Contract has {declared_delimiter}",
                        "expectedValue": "Delimiters should match",
                        "actualValue": declared_delimiter,
                        "location": f"Sample File {dataset_file_prefix}",
                        "issueDesc": "Mismatch in delimiter"
                    }
                    sample_file_issues.append(issue)

                sample_header = df_reader.columns.get_level_values(0).tolist()
                normalized_attributes = [attr.strip().upper() for attr in attributes]
                sample_header = [col.strip().upper() for col in sample_header]

                if ','.join(sample_header) != ','.join(normalized_attributes):
                    issue = {
                        "type": "ERROR",
                        "issueValue": "Sample File header does not match Data Contract",
                        "expectedValue": f"Header should be: {','.join(normalized_attributes)}",
                        "actualValue": ','.join(sample_header),
                        "location": f"Sample File {dataset_file_prefix}",
                        "issueDesc": "Mismatch between sample file columns and data contract attributes"
                    }
                    sample_file_issues.append(issue)

        except Exception as e:
            log.error("Error reading sample file", exc_info=True)
            issue = {
                "type": "ERROR",
                "issueValue": f"Could not read or analyze sample file for {dataset_file_prefix}",
                "expectedValue": "Accessible and readable UTF-8 sample file",
                "actualValue": str(e),
                "location": f"Sample File {dataset_file_prefix}",
                "issueDesc": "Exception during sample file analysis"
            }
            sample_file_issues.append(issue)

    if sample_file_issues:
        context['all_issues'].append({
            "Location": "Sample Files",
            "issues": sample_file_issues
        })


def predict_encoding(file_path, n_lines=10):
    with open(file_path, 'rb') as f:
        rawdata = b''.join([f.readline() for _ in range(n_lines)])
    return chardet.detect(rawdata)['encoding']





def validate_attribute_primary_key(value, count, dfs, row_index, context):
    location = findLoc(dfs, 'Attribute Primary Key', row_index)
    context.setdefault('primary_keys', []).append(value)
    if str(value).lower() not in context['template']['attributeBool']:
        if str(value).lower() == 'nan':
            value = '*This field is mandatory'
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Attribute Primary Key',
            "expectedValue": "Attribute Primary Key should be either Yes or No",
            "actualValue": value,
            "location": location,
            "issueDesc": "NA is considered no value. Field is mandatory."
        }
        context.setdefault('attribute_primary_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count']:
        if "INSUPD" in map(str.upper, context.get('ingestion_type_values', [])) and "YES" not in map(str.upper, context.get('primary_keys', [])):
            issue = {
                "type": "ERROR",
                "issueValue": 'Missing Primary Key for INSUPD',
                "expectedValue": "Attribute Primary Key is mandatory for INSUPD",
                "actualValue": value,
                "location": location,
                "issueDesc": "Attribute Primary Key must be set to YES for INSUPD ingestion."
            }
            context.setdefault('attribute_primary_logic_issues', []).append(issue)
            log.error(issue['issueValue'], extra=issue)

        if context.get('attribute_primary_issues'):
            context['all_issues'].append({
                "Location": "Attribute Primary Key",
                "issues": context['attribute_primary_issues']
            })

        if context.get('attribute_primary_logic_issues'):
            context['all_issues'].append({
                "Location": "Attribute Primary Key",
                "issues": context['attribute_primary_logic_issues']
            })

def validate_dataset_classification(classification, count, dfs, row_index, attribute_classifications, context):
    location = findLoc(dfs, 'Data Classification Type', row_index)
    context.setdefault('dataset_classification_values', []).append(classification)

    def classification_issue(msg):
        return {
            "type": "ERROR",
            "issueValue": 'The issue in the Classification.',
            "expectedValue": msg,
            "actualValue": classification,
            "location": location,
            "issueDesc": "Classification should match expected values."
        }

    if str(classification).lower() not in map(str.lower, context['template']['classification']):
        context.setdefault('dataset_classification_issues', []).append(
            classification_issue("Classification should match template options")
        )
        log.error('The issue in the Classification.', extra=context['dataset_classification_issues'][-1])
    else:
        attribute_classes = [str(attr).strip().lower() for attr in attribute_classifications]
        if 'sensitive' in attribute_classes and classification.lower() != 'sensitive':
            context.setdefault('dataset_classification_issues', []).append(
                classification_issue("Expected: Sensitive (based on attribute classifications)")
            )
        elif 'confidential' in attribute_classes and 'sensitive' not in attribute_classes:
            if classification.lower().strip() != 'confidential':
                context.setdefault('dataset_classification_issues', []).append(
                    classification_issue("Expected: Confidential (based on attribute classifications)")
                )
        for issue in context.get('dataset_classification_issues', []):
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('dataset_classification_issues'):
        context['all_issues'].append({
            "Location": "Dataset Classification Issue",
            "issues": context['dataset_classification_issues']
        })


        
def validate_data_contract_type(contract_type, count, dfs, row_index, context):
    location = findLoc(dfs, 'DataContract Type', row_index)
    if str(contract_type).lower() not in ['new', 'revised']:
        if str(contract_type).lower() == 'nan':
            contract_type = '*This field is mandatory'
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Split Logic',
            "expectedValue": "Data Contract Type should be New or Revised",
            "actualValue": contract_type,
            "location": location,
            "issueDesc": "Data Contract Type is mandatory and should be New or Revised"
        }
        context.setdefault('data_contract_type_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('data_contract_type_issues'):
        context['all_issues'].append({
            "Location": "Split Logic Key",
            "issues": context['data_contract_type_issues']
        })

def validate_frequency(frequency, count, dfs, column_name, row_index, context):
    location = findLoc(dfs, column_name, row_index)
    if str(frequency).lower() not in map(lambda x: x.lower(), context['template']['frequency']):
        if str(frequency).lower() == 'nan':
            frequency = '*This field is mandatory'
            issue = {
                "type": "ERROR",
                "issueValue": f'The issue in the {column_name}',
                "expectedValue": f"{column_name} should be {context['template']['frequency']}",
                "actualValue": frequency,
                "location": location,
                "issueDesc": f"{column_name} is mandatory and should be {context['template']['frequency']}"
            }
            context['frequency_issues'].append(issue)
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count']:
        if context['frequency_issues']:
            context['all_issues'].append({
                "Location": "Split Logic Key",
                "issues": context['frequency_issues']
            })

def validate_connectivity_option(connectivity, count, dfs, row_index, context):
    location = findLoc(dfs, 'Connectivity Option', row_index)
    if str(connectivity).lower() not in map(str.lower, context['template']['connectivity']):
        if str(connectivity).lower() != 'nan':
            issue = {
                "type": "ERROR",
                "issueValue": 'The issue in the Connectivity Option.',
                "expectedValue": f"Connectivity should match with {listToComma(context['template']['connectivity'])}",
                "actualValue": connectivity,
                "location": location,
                "issueDesc": "Connectivity should match expected values."
            }
            context.setdefault('connectivity_option_issues', []).append(issue)
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('connectivity_option_issues'):
        context['all_issues'].append({
            "Location": "Connectivity Issue",
            "issues": context['connectivity_option_issues']
        })

def validate_connectivity_description(description, count, dfs, row_index, context):
    location = findLoc(dfs, 'Description for Connectivity', row_index)
    context.setdefault('connectivity_descriptions', []).append(description)
    if str(description).lower() not in map(str.lower, context['template']['connectivityDesc']):
        if str(description).lower() == 'nan':
            if any(str(x).lower() != 'nan' for x in context['connectivity_descriptions']):
                return
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Connectivity Description.',
            "expectedValue": f"Connectivity Description should match with {listToComma(context['template']['connectivityDesc'])}",
            "actualValue": description,
            "location": location,
            "issueDesc": "Connectivity Description should match expected values."
        }
        context.setdefault('connectivity_desc_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('connectivity_desc_issues'):
        context['all_issues'].append({
            "Location": "Connectivity Description Issue",
            "issues": context['connectivity_desc_issues']
        })

def validate_delimiter(delimiter, other_value, count, dfs, row_index, context):
    location = findLoc(dfs, 'Attribute Delimiter', row_index)
    context.setdefault('delimiters', []).append(delimiter)
    if any(str(d).lower() != 'nan' for d in context['delimiters']):
        if str(delimiter) != 'nan':
            if str(delimiter) != 'Other':
                context.setdefault('delimiter_values', []).append(str(delimiter))
            else:
                context['delimiter_values'].append(str(other_value))
    else:
        if str(delimiter).lower() == 'nan':
            issue = {
                "type": "ERROR",
                "issueValue": 'The issue in the Delimiter.',
                "expectedValue": "Delimiter is mandatory. Merge if needed.",
                "actualValue": delimiter,
                "location": location,
                "issueDesc": "Delimiter column must be merged for the dataset."
            }
            context.setdefault('delimiter_issues_merge', []).append(issue)
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('delimiter_issues_merge'):
        context['all_issues'].append({
            "Location": "Delimiter Issue",
            "issues": context['delimiter_issues_merge']
        })

def validate_attribute_classification(classification, count, dfs, row_index, context):
    location = findLoc(dfs, 'Attribute Classification', row_index)
    if str(classification).lower() not in context['template']['classification']:
        if str(classification).lower() == 'nan':
            classification = '*This field is mandatory'
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Attribute Classification',
            "expectedValue": "Attribute Classification should be either Open, Confidential, Sensitive",
            "actualValue": classification,
            "location": location,
            "issueDesc": "Mandatory field. NA is considered empty."
        }
        context.setdefault('attribute_classification_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('attribute_classification_issues'):
        context['all_issues'].append({
            "Location": "Attribute Classification",
            "issues": context['attribute_classification_issues']
        })

def validate_service(service, count, dfs, row_index, context):
    location = findLoc(dfs, 'Service', row_index)
    context.setdefault('services', []).append(service)
    if not any(str(s).lower() != 'nan' for s in context['services']):
        if str(service).lower() == 'nan':
            issue = {
                "type": "ERROR",
                "issueValue": 'The issue in the Service.',
                "expectedValue": "Service is mandatory. Merge service columns if needed.",
                "actualValue": service,
                "location": location,
                "issueDesc": "Service column should be merged and contain a valid value."
            }
            context.setdefault('service_issues_merge', []).append(issue)
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('service_issues_merge'):
        context['all_issues'].append({
            "Location": "Service Issue",
            "issues": context['service_issues_merge']
        })

def validate_category(category, count, dfs, row_index, context):
    location = findLoc(dfs, 'Category', row_index)
    context.setdefault('categories', []).append(category)
    if not any(str(c).lower() != 'nan' for c in context['categories']):
        if str(category).lower() == 'nan':
            issue = {
                "type": "ERROR",
                "issueValue": 'The issue in the Category.',
                "expectedValue": "Category is mandatory. Merge category columns if needed.",
                "actualValue": category,
                "location": location,
                "issueDesc": "Category column should be merged and contain a valid value."
            }
            context.setdefault('category_issues_merge', []).append(issue)
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('category_issues_merge'):
        context['all_issues'].append({
            "Location": "Category Issue",
            "issues": context['category_issues_merge']
        })

def validate_entity(entity, count, dfs, row_index, context):
    location = findLoc(dfs, 'Entity', row_index)
    context.setdefault('entities', []).append(entity)
    if not any(str(e).lower() != 'nan' for e in context['entities']):
        if str(entity).lower() == 'nan':
            issue = {
                "type": "ERROR",
                "issueValue": 'The issue in the Entity.',
                "expectedValue": "Entity is mandatory. Merge entity columns if needed.",
                "actualValue": entity,
                "location": location,
                "issueDesc": "Entity column should be merged and contain a valid value."
            }
            context.setdefault('entity_issues_merge', []).append(issue)
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('entity_issues_merge'):
        context['all_issues'].append({
            "Location": "Entity Issue",
            "issues": context['entity_issues_merge']
        })

def validate_data_type(data_type, count, dfs, row_index, context):
    location = findLoc(dfs, 'Attribute DataType', row_index)
    if str(data_type).lower() not in context['template']['dataTypes']:
        if str(data_type).lower() == 'nan':
            data_type = '*This field is mandatory'
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Data Type',
            "expectedValue": "Data Type should match with " + listToComma(context['template']['dataTypes']),
            "actualValue": data_type,
            "location": location,
            "issueDesc": "Data Type should match expected values. Don't include (size); use the size column."
        }
        context.setdefault('data_type_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('data_type_issues'):
        context['all_issues'].append({
            "Location": "Data Type",
            "issues": context['data_type_issues']
        })

def validate_data_type_size(data_type_size, data_type, count, dfs, row_index, context):
    location = findLoc(dfs, 'Attribute Size', row_index)
    if str(data_type_size).lower() == 'nan':
        if re.match("date|time", str(data_type).lower()) or str(data_type).lower() in context['template']['numberDataTypeSize']:
            return
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Data Type Size',
            "expectedValue": f"Size should be available for {data_type} (except date/time)",
            "actualValue": '*This field is mandatory',
            "location": location,
            "issueDesc": "Size is mandatory for non-date/time types."
        }
        context.setdefault('data_type_size_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)
    else:
        size = str(data_type_size).split('.')[0]
        if not re.match("^[0-9,]*$", size):
            issue = {
                "type": "ERROR",
                "issueValue": 'The issue in the Data Type Size',
                "expectedValue": f"Only numbers and comma allowed for {data_type}",
                "actualValue": data_type_size,
                "location": location,
                "issueDesc": "Size must be numeric or comma-separated (for floats)."
            }
            context.setdefault('data_type_size_issues', []).append(issue)
            log.error(issue['issueValue'], extra=issue)
        elif size == '0' or (',' in size and size.split(',')[0] == '0'):
            issue = {
                "type": "ERROR",
                "issueValue": 'The issue in the Data Type Size',
                "expectedValue": "Precision cannot be zero(0)",
                "actualValue": size,
                "location": location,
                "issueDesc": "Precision should be greater than zero."
            }
            context.setdefault('data_type_size_issues', []).append(issue)
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('data_type_size_issues'):
        context['all_issues'].append({
            "Location": "DataType Size",
            "issues": context['data_type_size_issues']
        })

def validate_date_format(attribute_range, data_type, count, dfs, row_index, context):
    location = findLoc(dfs, 'Attribute Range of Values', row_index)
    if re.match("date|time", str(data_type).lower()):
        if str(attribute_range).lower() == 'nan' or str(attribute_range).upper() not in context['template']['dateformat']:
            issue = {
                "type": "ERROR",
                "issueValue": 'The issue in the Date/Time Format',
                "expectedValue": "Date/Time Format should be one of UDF_TO_DATETIME valid formats.",
                "actualValue": attribute_range,
                "location": location,
                "issueDesc": "Date/Time Format not matching valid formats."
            }
            context.setdefault('date_format_issues', []).append(issue)
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('date_format_issues'):
        context['all_issues'].append({
            "Location": "DateTime Format",
            "issues": context['date_format_issues']
        })

def validate_attribute(attribute, count, dfs, row_index, context):
    location = findLoc(dfs, 'Attribute', row_index)
    if not re.match("^[a-zA-Z0-9_ ]*$", str(attribute)):
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue exists in the Attribute Name',
            "expectedValue": "Special Characters are not allowed like ( ) % $ ^. Allowed characters are 0-9, a-z, A-Z and underscore.",
            "actualValue": attribute,
            "location": location,
            "issueDesc": "Attribute names must only include alphanumeric characters, spaces, or underscores."
        }
        context['attribute_issues'].append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count']:
        if context['attribute_issues']:
            context['all_issues'].append({
                "Location": "Attribute",
                "issues": context['attribute_issues']
            })

def validate_encoding(encoding, count, dfs, row_index, context):
    context['encoding_values'] = context.get('encoding_values', [])
    context['encoding_values'].append(encoding)
    location = findLoc(dfs, 'Code Page', row_index)
    if all(str(x).lower() == 'nan' for x in context['encoding_values']):
        if str(encoding).lower() != 'utf-8':
            if str(encoding).lower() == 'nan':
                issue = {
                    "type": "ERROR",
                    "issueValue": 'The issue in the Encoding.',
                    "expectedValue": "Encoding is mandatory. If encoding exists in separate column, merge the encoding columns",
                    "actualValue": encoding,
                    "location": location,
                    "issueDesc": "Encoding Column should be merged for the Dataset, it should be a single value."
                }
            else:
                issue = {
                    "type": "ERROR",
                    "issueValue": 'The issue in the Encoding.',
                    "expectedValue": "Encoding should be UTF-8",
                    "actualValue": encoding,
                    "location": location,
                    "issueDesc": "Code Page Column should be UTF-8 without Spaces."
                }
            context['encoding_issues'].append(issue)
            log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count']:
        if context['encoding_issues']:
            context['all_issues'].append({
                "Location": "Encoding Issue",
                "issues": context['encoding_issues']
            })

def validate_format(format_value, count, dfs, row_index, context):
    location = findLoc(dfs, 'Format (MIME)', row_index)
    if str(format_value).lower() not in map(lambda x: x.lower(), context['template']['format']):
        if str(format_value).lower() == 'nan':
            format_value = '*This field is mandatory'
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Format',
            "expectedValue": "Format should have one of values:" + listToComma(context['template']['format']),
            "actualValue": format_value,
            "location": location,
            "issueDesc": "NA will be considered as no value for mandatory field, and it is mandatory to fill the Format."
        }
        context['format_issues'].append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count']:
        if context['format_issues']:
            context['all_issues'].append({
                "Location": "Format",
                "issues": context['format_issues']
            })

def validate_split_logic(split_logic, count, dfs, row_index, context):
    location = findLoc(dfs, 'Split Logic', row_index)
    if str(split_logic).lower() not in map(str.lower, context['template']['splitLogic']):
        if str(split_logic).lower() == 'nan':
            split_logic = '*This field is mandatory'
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Split Logic',
            "expectedValue": f"Split Logic should be {context['template']['splitLogic']}",
            "actualValue": split_logic,
            "location": location,
            "issueDesc": f"Split Logic is mandatory and should be {context['template']['splitLogic']}"
        }
        context.setdefault('split_logic_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('split_logic_issues'):
        context['all_issues'].append({
            "Location": "Split Logic Key",
            "issues": context['split_logic_issues']
        })

def validate_ingestion_type(ingestion_type, count, dfs, row_index, context):
    location = findLoc(dfs, 'Ingestion Logic', row_index)
    context.setdefault('ingestion_type_values', []).append(ingestion_type)
    if str(ingestion_type).lower() not in map(str.lower, context['template']['ingestionType']):
        if str(ingestion_type).lower() == 'nan':
            if any(str(x).lower() != 'nan' for x in context['ingestion_type_values']):
                return
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Ingestion Type',
            "expectedValue": f"Ingestion Type should match with {listToComma(context['template']['ingestionType'])}",
            "actualValue": ingestion_type,
            "location": location,
            "issueDesc": "Ingestion Type should match one of the allowed values."
        }
        context.setdefault('ingestion_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('ingestion_issues'):
        context['all_issues'].append({
            "Location": "Ingestion Issue",
            "issues": context['ingestion_issues']
        })

def validate_attribute_nullability(value, count, dfs, row_index, context):
    location = findLoc(dfs, 'Attribute Nullability', row_index)
    if str(value).lower() not in context['template']['attributeBool']:
        if str(value).lower() == 'nan':
            value = '*This field is mandatory'
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Attribute Nullability',
            "expectedValue": "Attribute Nullability should be either Yes or No",
            "actualValue": value,
            "location": location,
            "issueDesc": "NA is considered no value. Field is mandatory."
        }
        context.setdefault('attribute_nullability_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('attribute_nullability_issues'):
        context['all_issues'].append({
            "Location": "Attribute Nullability",
            "issues": context['attribute_nullability_issues']
        })

def validate_attribute_uniqueness(value, count, dfs, row_index, context):
    location = findLoc(dfs, 'Attribute Uniqueness', row_index)
    if str(value).lower() not in context['template']['attributeBool']:
        if str(value).lower() == 'nan':
            value = '*This field is mandatory'
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Attribute Uniqueness',
            "expectedValue": "Attribute Uniqueness should be either Yes or No",
            "actualValue": value,
            "location": location,
            "issueDesc": "NA is considered no value. Field is mandatory."
        }
        context.setdefault('attribute_uniqueness_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('attribute_uniqueness_issues'):
        context['all_issues'].append({
            "Location": "Attribute Uniqueness",
            "issues": context['attribute_uniqueness_issues']
        })

def validate_language(language, count, dfs, row_index, context):
    location = findLoc(dfs, 'Language', row_index)
    if str(language).lower() not in context['template']['language'] and str(language).lower() != 'nan':
        issue = {
            "type": "ERROR",
            "issueValue": 'The issue in the Language',
            "expectedValue": "Language should be either English, Arabic, Mix or Other",
            "actualValue": language,
            "location": location,
            "issueDesc": "Language should be either English, Arabic, Mix or Other. NA is considered no value."
        }
        context.setdefault('language_issues', []).append(issue)
        log.error(issue['issueValue'], extra=issue)

    if count['index'] == count['count'] and context.get('language_issues'):
        context['all_issues'].append({
            "Location": "Language",
            "issues": context['language_issues']
        })
        
        
      
    

def write_issues(file, context):
    if context['issues']:
        issues_json_path = os.path.join(CODES_DIR, 'issues', 'issues.json')
        with open(issues_json_path, 'w') as f:
            json.dump(context['issues'], f, indent=4, sort_keys=True)

        if len(context['issues']) > 2:
            with open(os.path.join(CODES_DIR, 'issues', 'checkIssue.txt'), 'w') as f:
                f.write(json.dumps(context['issues'], indent=4))



def generate_report(file, context):
    j2_env = Environment(
        loader=FileSystemLoader(CODES_DIR),
        trim_blocks=True,
        autoescape=False
    )
    issues_data = context.get('all_issues', [])
    unique_issues = []
    for issue in issues_data:
        if issue not in unique_issues:
            unique_issues.append(issue)

    output = j2_env.get_template('template/report.html').render(
        issues=unique_issues,
        _otherIssues=context.get('other_issues', [])
    )
    report_dir = os.path.join(CODES_DIR, 'reports')
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{file.split('.')[0]}_report.html")
    with open(report_path, 'w', encoding="utf-8") as f:
        f.write(output)

def generateReport(file):
	j2_env = Environment(loader=FileSystemLoader(CODES_DIR),
						trim_blocks=True,autoescape=False)
	if Issues:
		data  = json.loads(Issues)
	else:
		data = []
	final_list = [] 
	for i in range(len(data)): 
		if data[i] not in data[i + 1:]: 
			final_list.append(data[i]) 
	output =  j2_env.get_template('template/report.html').render(
		issues=final_list,_otherIssues=otherIssues
	)
	REPORT_DIR = CODES_DIR +'/reports/'
	report_path = os.path.join(REPORT_DIR, f"{file.split('.')[0]}_report.html")
	with open(report_path, 'w', encoding="utf-8") as f:  
		f.write("%s\n" % output)



def main():
    args = read_input_args()
    context = initialize_context()
    context['connection'], context['cursor'] = connect_to_postgres(
        args.psql_db, args.psql_user, args.psql_password, args.psql_host, args.psql_port
    )
    if not context['connection']:
        sys.exit(1)

    
    process_file(args.file, args.dir, context)

if __name__ == "__main__":
    main()

