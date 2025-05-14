'''
	Main Declarations that is required by the Module.
'''
# Time module to check total execution time of the code
import time
start = time.time()

#Python Argument Parser
import argparse
#Python JSON Library
import json
#Pandas Library for parsing the Data Contract
import pandas as pd
#Regular Expression Library
import re
#Python Sys Libraries
import sys
import os
import csv
'''Predict a file's encoding using chardet'''
import chardet
#Connecting Python to SQL Server using pyodbc
import pyodbc
import psycopg2
import configparser

#Templating
from jinja2 import Environment, FileSystemLoader

#default Confifurations
from configuration import log, DATA_CONTRACT_DEFAULT_PATH, PARSED_DATA_CONTRACT_DEFAULT_PATH, TEMPLATE, CODES_DIR
from lib.utils import strippedText, camelCase, diff, snakeCase, column_string, findLoc, listToComma
FILES = []

#Global Variables
Issues = []
otherIssues = []
allIssues = []
#Import Template File
mappingJsonFile = json.load(TEMPLATE)
#Move Cursor to start of the File
TEMPLATE.seek(0)

# Capture current directory
THIS_DIR = os.path.dirname(os.path.abspath(__file__))


'''
	Get the path of the file from the parameters.
	Default path is inside 'Data_Contracts' from the config file
	return : Path of the file to be read and File Name

'''
def readInputArgs():
	try:
		log.info('Data Contract File Path Function: Started')
		parser = argparse.ArgumentParser(description='The Arguments --file, --dir and postgres creds are required')
		parser.add_argument('--file', '-f', help="Data Contract File Name ", type=str, required=True)
		parser.add_argument('--dir', '-d', help="Data Contract Directory Path", type=str, default=DATA_CONTRACT_DEFAULT_PATH)
		parser.add_argument('--psql_db', '-psql_db', help="Postgres Database ", type=str, required=True)
		parser.add_argument('--psql_user', '-psql_user', help="Postgres Username", type=str, required=True)
		parser.add_argument('--psql_password', '-psql_pwd', help="Postgres Password", type=str, required=True)
		parser.add_argument('--psql_host', '-psql_host', help="Postgres Hostname", type=str, required=True)
		parser.add_argument('--psql_port', '-psql_port', help="Postgres Port", type=str, required=True)
		args = parser.parse_args()
		return args.file, args.dir, args.psql_db, args.psql_user, args.psql_password, args.psql_host, args.psql_port
	except Exception as e: 
		# sys.exit(1)
		log.error("Error in reading the input arguments", exc_info=True)

'''
	Use : Header Issues
	Desc : Finding the issues with the Enitity that doesn't exist in Entity Database.
	@Params : file - FileName that need to be read by Pandas
	returns : entityName
'''

def entityName(file, psql_db, psql_user, psql_password, psql_host, psql_port):
	try:
		if '_' in file:
			entityName = file.split('_')[0].upper()
			log.info("Entity Name from Data Contract: %s", entityName)

			global ENTITY
			global cursor
			connection, cursor = connect_to_postgres(psql_db, psql_user, psql_password, psql_host, psql_port)
			query = "Select ENTITY_NAME FROM abc_db.ENTITY_MAPPING WHERE ENTITY_NAME = '"+entityName+"';"
			cursor.execute(query)
			result = cursor.fetchone()
			print(cursor, result)
			if result == None:
				log.error("The Entity is not present in ENTITY_MAPPING or is not a valid Entity")
				issue = {
					"type":"ERROR",
					"Location":"ENTITY Check in DB",
					"issueValue":'ENTITY is not exists in DB - ENTITY_MAPPING',
					"expectedValue":"ENTITY Should be there in DB as : " + str(entityName),
					"actualValue":entityName,
					"issueDesc":"Add new ENTITY in Database or Check the ENTITY name spelling."
				}
				otherIssues.append(issue)
				log.error(issue['issueValue'],extra= issue)
				generateReport(file)
				sys.exit(1)
			else:
				ENTITY = result[0]
				log.info(" ENTITY from DB  is Matching : %s", ENTITY)
				return ENTITY
		else: 
			issue = {
				"type":"ERROR",
				"Location":"Entity Name Check",
				"issueValue":'The issue in name of Entity Name in FileName',
				"expectedValue":"ENTITY_NAME_CONTRACTNAME_SPRINT5.xlsx",
				"actualValue":entityName,
				"issueDesc":"Check Enitity Name in Database (config/entitiy.txt) file or Check the file name spelling."
			}
			otherIssues.append(issue)
			log.error(issue['issueValue'],extra= issue)
			generateReport(file)
			sys.exit(1)
	except Exception as e: 
		generateReport(file)
		log.error("Exception occurred while getting Data Contract Path", exc_info=True)
		sys.exit(1)	


'''
	Use : Execute the Program Functions, Write Issues and Create Report
	@Params: file - Type:String
			data_contract_path - Type:String
'''
def execute(file,data_contract_path):
	try:
		global Issues # Creating Global Variable to store all the issues for all the Data Contracts
		global otherIssues #Other issues is used for issues which are not part of Data Contract like sample Files
		Issues = [] #
		otherIssues = []
		data_contract = data_contract_path + file
		_, _, psql_db, psql_user, psql_password, psql_host, psql_port = readInputArgs()
		entityName(file, psql_db, psql_user, psql_password, psql_host, psql_port) # Find the entity Name in Entity table
		sheetName(file) # Check Whether Sheet name format is correct or not 
		dfs = parseContract(data_contract) # Parse the Data contract for Issues and return Data Frame
		header(dfs,file) # Check The Headers for Issues
		parseDatasets(dfs) # Parse the Dataset from the Data Frames
		print(Issues)
		with open(CODES_DIR+'/issues/issues.json', 'w') as f:  
			Issues = json.dumps(Issues, indent=4, sort_keys=True)
			if len(Issues)>2:
				with open(CODES_DIR+'/issues/checkIssue.txt', 'w') as i_f:
					i_f.write(Issues)
			f.write("%s\n" % Issues) # Write issues to File
		generateReport(file) # Generate Report if issues
	except Exception as e:
		log.error("Error in execute Function - checkDataContract.py", exc_info=True)

'''
	Use : Header Issues
	Desc : Finding the issues with the sheetName that doesn't match with the standard.
	@Params : file - FileName that need to be read by Pandas
	returns : True/raise Error
'''
def sheetName(file):
	try:
		log.info("Entity is passed in the entity name function")
		log.info("Sheet Name Check Started")
		sprint = file.split('.')[-2].split('_')[-1]
		match = re.match(r"([a-z]+)([0-9]+)", sprint, re.I)
		if match:
			items = match.groups()
			if list(items)[0].lower() == 'sprint' and list(items)[1].isdigit():
				pass
			else:
				raise		
	except:
		issue = {
			"type":"ERROR",
			"Location": "Header Check",
			"issueValue":'The issue exists in name of Data Contract File',
			"expectedValue":"ENTITYNAME_FILENAME_SPRINT_NUMBER.xlsx",
			"actualValue":file,
			"issueDesc":"The issue can be with the spelling or structure of Data Contract Name."
		}
		otherIssues.append(issue)
		log.error(issue['issueValue'],extra= issue)
		pass

'''
	Parse Data Contract using Pandas
	@Params : data_contract - Data Contract Path
	return : dfs - Data Frame
'''
def parseContract(data_contract):
	log.info('Check Sheet Function: Started')
	log.info('Data Contract Location: %s',data_contract)
	dfs = pd.read_excel(data_contract, sheet_name="Metadata Template", header=2) #header 2 due to data Contract has top two rows which are not considered as header for parsing.
	return dfs


'''
	Header , SheetName and Entity Name is Excel level issue and need to handle it differently

'''

'''
	
	@Params : dfs - Data Frame  - output of excel read from Pandas.
	return : Append to Issue Block
'''
'''
	Use : Header Issues
	Desc : Finding the issues with the header that doesn't match with the standard.
	@Params : dfs - Data Frame  - output of excel read from Pandas.
			  file - File Name
	returns : True/raise Error
'''
def header(dfs,file):  #-- check if the columns in the data contracts matches the template
	try: 
		idx = pd.Index(dfs.columns) #find indexes
		idx = list(idx)
		TEMPLATE.seek(0) #Visit the top of the File (Template File)
		cols = json.load(TEMPLATE)['header']
		'''
			MatchedCols is the comparision of the attributes available and matching with standard set
		'''
		matchedCols = set(idx) & set(cols)
		if len(list(matchedCols)) == len(cols):
			log.info("The File Doesn't have issue with the headers")
			return True
		else:
			missingValue = set(cols) - set(matchedCols)
			expectedValue = diff(cols,idx)
			actualValue = diff(idx,cols)
			issue = {
						"type":"ERROR",
						"Location": "Header Check",
						"issueValue":'The issue exists at the header of the Data Contract',
						"expectedValue":list(expectedValue),
						"actualValue":list(actualValue),
						"issueDesc":"The issue can be with the spelling or spaces or missing Column in the Data Contract."
					}
			otherIssues.append(issue)
			log.error(issue['issueValue'],extra=issue)
			raise ValueError("Issue with the Headers")
	except Exception as e: 
		generateReport(file)
		log.error("Exception occurred while getting Data Contract Path", exc_info=True)
		sys.exit(1)

'''
	Use : Empty the list of Global Variables
	Desc : Clean and Generate the Global Values to store the data of the list.
	@Params : None
	returns : None
'''
def cleanIndexes():
	global AttributeIndex
	global LanguageIndex
	global allIssues
	global DatasetNameIndex
	global ClassificationIndex
	global AttributeNullabilityIndex
	global AttributePrimaryIndex
	global AttributePrimaryLogicIndex
	global AttributeUniquenessIndex
	global AttributeDescriptionIndex
	global DataTypeIndex
	global DataTypeSizeIndex
	global PrimaryIdentifierIndex
	global UniqueIdentifierIndex
	global ConnectivityIndex
	global EncodingIndex
	global DelimiterIndex
	global ConnectivityDescIndex
	global IngestionTypeIndex
	global SampleFileIndex
	global DatasetClassificationIndex
	global INGESTION_TYPE
	global PRIMARY_KEY
	global Connectivity_TYPE
	global DATASET_CLASSIFICATION
	global ENCODING
	global CATEGORY
	global SERVICE
	global ENTITYDC
	global DELIMITER
	global DELIMITER_VALUE
	global ServiceIndex
	global CategoryIndex
	global EntityIndex
	global DateTimeFormatIndex
	global FormatIndex
	global SplitLogicIndex
	global FrquencyIndex
	global DataContractTypeIndex
	FrquencyIndex = []
	DataContractTypeIndex = []
	SplitLogicIndex = []
	FormatIndex = []
	AttributeIndex = []
	LanguageIndex = []
	allIssues = []
	DatasetNameIndex = []
	ClassificationIndex = []
	AttributeNullabilityIndex = []
	AttributePrimaryIndex = []
	AttributePrimaryLogicIndex = []
	AttributeUniquenessIndex = []
	AttributeDescriptionIndex = []
	DataTypeIndex = []
	DataTypeSizeIndex = []
	PrimaryIdentifierIndex = []
	UniqueIdentifierIndex = []
	ConnectivityIndex = []
	EncodingIndex = []
	DelimiterIndex = []
	ConnectivityDescIndex = []
	IngestionTypeIndex = []
	SampleFileIndex = []
	DatasetClassificationIndex = []
	INGESTION_TYPE = []
	PRIMARY_KEY = []
	Connectivity_TYPE = []
	DATASET_CLASSIFICATION = []
	ENCODING = []
	CATEGORY = []
	SERVICE = []
	ENTITYDC = []
	DELIMITER = []
	DELIMITER_VALUE = []
	ServiceIndex = []
	CategoryIndex = []
	EntityIndex = []
	DateTimeFormatIndex = []


'''
	Check the Frequency for the issues. Frequency should match with the values in template.json
	@Params : _frequency - Value of Frequency
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	
def __frequency(_frequency, count, dfs, column_name, _):
	location = findLoc(dfs,column_name,_)
	global _FrequencyIssue_
	if str(_frequency).lower() not in map(lambda x: x.lower(), mappingJsonFile['frequency']) :
		if str(_frequency).lower() == 'nan':
			_frequency = '*This field is mandatory'
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the ' + column_name,
					"expectedValue":column_name +" should be "+ str(mappingJsonFile['frequency']),
					"actualValue":_frequency,
					"location":location,
					"issueDesc":column_name + " is mandatory and should be "+ str(mappingJsonFile['frequency'])
				}
			FrquencyIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_FrequencyIssue_ = {}
	if count['index'] == count['count']:
		_FrequencyIssue_ = {
			"Location" : "Split Logic Key",
			"issues" : FrquencyIndex
		}
		if bool(_FrequencyIssue_['issues']):
			allIssues.append(_FrequencyIssue_)


'''
	Check the Data Contract Type for the issues. Data Contract Type should match with the values in template.json
	@Params : _dataContractType - Value of Data Contract Type
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	
def __dataContractType(_dataContractType, count, dfs, _):
	location = findLoc(dfs,'DataContract Type',_)
	global _DataContractTypeIssue_
	if str(_dataContractType).lower() not in ['new', 'revised'] :
		if str(_dataContractType).lower() == 'nan':
			_dataContractType = '*This field is mandatory'
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Split Logic',
					"expectedValue":"Data Contract Type should be New or Revised",
					"actualValue":_dataContractType,
					"location":location,
					"issueDesc":"Data Contract Type is mandatory and should be New or Revised"
				}
			DataContractTypeIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_DataContractTypeIssue_ = {}
	if count['index'] == count['count']:
		_DataContractTypeIssue_ = {
			"Location" : "Split Logic Key",
			"issues" : DataContractTypeIndex
		}
		if bool(_DataContractTypeIssue_['issues']):
			allIssues.append(_DataContractTypeIssue_)

'''
	Check the Split Logic Key for the issues. Split Logic Key should match with the values in template.json
	@Params : _splitLogic - Value of Split Logic
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	
def __splitLogic(_splitLogic, count, dfs, _):
	location = findLoc(dfs,'Split Logic',_)
	global _SplitLogicIssue_
	if str(_splitLogic).lower() not in map(lambda x: x.lower(), mappingJsonFile['splitLogic']) :
		if str(_splitLogic).lower() == 'nan':
			_splitLogic = '*This field is mandatory'
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Split Logic',
					"expectedValue":"Split Logic should be "+ str(mappingJsonFile['splitLogic']),
					"actualValue":_splitLogic,
					"location":location,
					"issueDesc":"Split Logic is mandatory and should be "+ str(mappingJsonFile['splitLogic'])
				}
			SplitLogicIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_SplitLogicIssue_ = {}
	if count['index'] == count['count']:
		_SplitLogicIssue_ = {
			"Location" : "Split Logic Key",
			"issues" : SplitLogicIndex
		}
		if bool(_SplitLogicIssue_['issues']):
			allIssues.append(_SplitLogicIssue_)

'''
	Check the Format for the issues. Format should match with the values in template.json
	@Params : _formatValue - Format
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	
def __format(_formatValue, count, dfs, _):
	location = findLoc(dfs,'Format (MIME)',_)
	global _FormatIssue_
	if str(_formatValue).lower() not in map(lambda x: x.lower(), mappingJsonFile['format']):
		if str(_formatValue).lower() == 'nan':
			_formatValue = '*This field is mandatory'
		issue = {
				"type":"ERROR",
				"issueValue":'The issue in the Format',
				"expectedValue":"Format should have one of values:" + listToComma(mappingJsonFile['format'] ),
				"actualValue":_formatValue,
				"location":location,
				"issueDesc":"NA will be consider as No value for Mandatory Field and it is mandatory to fill the Format. "
			}
		FormatIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_FormatIssue_ = {}
	if count['index'] == count['count']:
		_FormatIssue_ = {
			"Location" : "Format",
			"issues" : FormatIndex
		}
		if bool(_FormatIssue_['issues']):
			allIssues.append(_FormatIssue_)

'''
	Check the Ingestion Logic for the issues.
	@Params : __ingestionType - Type of Ingestion - INSUPD/FULL/INCREMENTAL
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __ingestionType(_ingestionType, count, dfs, _):
	INGESTION_TYPE.append(_ingestionType)
	location = findLoc(dfs,'Ingestion Logic',_)
	global _IngestionTypeIssue_
	if str(_ingestionType).lower() not in map(lambda x: x.lower(), mappingJsonFile['ingestionType']) :
		if str(_ingestionType).lower() == 'nan':
			if True in list(map(lambda x:str(x) != 'nan',INGESTION_TYPE)):
				pass
			else:
				issue = {
						"type":"ERROR",
						"issueValue":'The issue in the Ingestion Type',
						"expectedValue":"Ingestion Type should be match with " + listToComma(mappingJsonFile['ingestionType'] ),
						"actualValue":_ingestionType,
						"location":location,
						"issueDesc":"Ingestion Type should be matched with expected value."
					}
				IngestionTypeIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
		else:
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Ingestion Type',
					"expectedValue":"Ingestion Type should be match with " + listToComma(mappingJsonFile['ingestionType'] ),
					"actualValue":_ingestionType,
					"location":location,
					"issueDesc":"Ingestion Type should be matched with expected value."
				}
			IngestionTypeIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_IngestionTypeIssue_ = {}
	if count['index'] == count['count']:
		_IngestionTypeIssue_ = {
			"Location" : "Ingestion Issue",
			"issues" : IngestionTypeIndex
		}
		if bool(_IngestionTypeIssue_['issues']):
			allIssues.append(_IngestionTypeIssue_)

'''
	Check the dataset Length, it should not exceed 50.
	@Params : _dataset - Dataset
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	
def __dataset(_dataset, count, dfs, _):
	location = findLoc(dfs,'Dataset Name',_)
	global _DatasetNameIssue_
	length = 50
	if len(_dataset) > length:
		issue = {
				"type":"ERROR",
				"issueValue":'The issue in the Dataset Length',
				"expectedValue":"Dataset Length should not be greater than 50.",
				"actualValue":_dataset + " has length: "+ str(len(_dataset)),
				"location":location,
				"issueDesc":"Dataset length should be less than 50."
		}
		if len(DatasetNameIndex) > 0:
			pass;
		else:
			DatasetNameIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
		_DatasetNameIssue_ = {}
		if count['index'] == count['count']:
			_DatasetNameIssue_ = {
				"Location" : "DatasetName",
				"issues" : DatasetNameIndex
			}
			if bool(_DatasetNameIssue_['issues']):
				allIssues.append(_DatasetNameIssue_)
	else:
		if re.match("^[a-zA-Z0-9_ ]*$", str(_dataset)):
			_DatasetNameIssue_ = {}
			pass;
		else:
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Dataset Name',
					"expectedValue":"Dataset Name should be english only.",
					"actualValue":str(_dataset),
					"location":location,
					"issueDesc":"Dataset Name should english only."
			}
			if len(DatasetNameIndex) > 0:
				pass;
			else:
				DatasetNameIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
			_DatasetNameIssue_ = {}
			if count['index'] == count['count']:
				_DatasetNameIssue_ = {
					"Location" : "DatasetName",
					"issues" : DatasetNameIndex
				}
				if bool(_DatasetNameIssue_['issues']):
					allIssues.append(_DatasetNameIssue_)

'''
	Check the attribute for the issues.
	@Params : _attribute - Attribute Name
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''

def __attribute(_attribute, count, dfs, _):
	location = findLoc(dfs,'Attribute',_)
	global _AttributeIssue_ 
	
	if re.match("^[a-zA-Z0-9_ ]*$", str(_attribute)):
		_AttributeIssue_ = {}
		pass;
	else:
		issue = {
				"type":"ERROR",
				"issueValue":'The issue exists in the Attribute Name',
				"expectedValue":"Special Characters are not allowed like ( ) % $ ^, allowed Characters are 0-9,a-z,A-z and Underscore",
				"actualValue":_attribute,
				"location":location,
				"issueDesc":"The issue will be with Attribute name. Special Characters are not allowed like ( ) % $ ^, allowed Characters are 0-9,a-z,A-z and Underscore. Check for any special character. "
			}
		AttributeIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
	_AttributeIssue_ = {}
	if count['index'] == count['count']:
		_AttributeIssue_ = {
			"Location" : "Attribute",
			"issues" : AttributeIndex
		}
		if bool(_AttributeIssue_['issues']):
			allIssues.append(_AttributeIssue_)

'''
	Check the Attribute Nullability for the issues. Attribute Nullability should match with the values in template.json
	@Params : _attributeValue - Attribute Nullability
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	
def __attributeNullability(_attributeValue, count, dfs, _):
	location = findLoc(dfs,'Attribute Nullability',_)
	global _AttributeNullabilityIssue_
	if str(_attributeValue).lower() not in mappingJsonFile['attributeBool']:
		if str(_attributeValue).lower() == 'nan':
			_attributeValue = '*This field is mandatory'
		issue = {
				"type":"ERROR",
				"issueValue":'The issue in the Attribute Nullability',
				"expectedValue":"Attribute Nullability should be either Yes or No",
				"actualValue":_attributeValue,
				"location":location,
				"issueDesc":"NA will be consider as No value for Mandatory Field and it is mandatory to fill the Attribute Nullability. "
			}
		AttributeNullabilityIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_AttributeNullabilityIssue_ = {}
	if count['index'] == count['count']:
		_AttributeNullabilityIssue_ = {
			"Location" : "Attribute Nullability",
			"issues" : AttributeNullabilityIndex
		}
		if bool(_AttributeNullabilityIssue_['issues']):
			allIssues.append(_AttributeNullabilityIssue_)

'''
	Check the Attribute Primary Key for the issues. Attribute Primary Key should match with the values in template.json
	@Params : _attributeValue - Attribute Primary Key
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	
def __attributePrimaryKey(_attributeValue, count, dfs, _):
	PRIMARY_KEY.append(_attributeValue)
	location = findLoc(dfs,'Attribute Primary Key',_)
	global _AttributePrimaryIssue_
	global _AttributePrimaryLogicIssue_
	if str(_attributeValue).lower() not in mappingJsonFile['attributeBool']:
		if str(_attributeValue).lower() == 'nan':
			_attributeValue = '*This field is mandatory'
		issue = {
				"type":"ERROR",
				"issueValue":'The issue in the Attribute Primary Key',
				"expectedValue":"Attribute Primary Key should be either Yes or No",
				"actualValue":_attributeValue,
				"location":location,
				"issueDesc":"NA will be consider as No value for Mandatory Field and it is mandatory to fill the Attribute Primary Key. "
			}
		AttributePrimaryIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)

	_AttributePrimaryIssue_ = {}
	_AttributePrimaryLogicIssue_ = {}
	if count['index'] == count['count']:
		_AttributePrimaryIssue_ = {
			"Location" : "Attribute Primary Key",
			"issues" : AttributePrimaryIndex
		}
		if bool(_AttributePrimaryIssue_['issues']):
			allIssues.append(_AttributePrimaryIssue_)
		if "INSUPD" in map(str.upper, filter(lambda v: v==v, INGESTION_TYPE)) and "YES" not in map(str.upper, PRIMARY_KEY):
			issue = {
				"type":"ERROR",
				"issueValue":'Issue in Attribute Primary key',
				"expectedValue":"Attribute Primary Key is mandatory for INSUPD",
				"actualValue":_attributeValue,
				"location":location,
				"issueDesc":"Attribute Primary Key is mandatory for INSUPD"
			}
			AttributePrimaryLogicIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
			_AttributePrimaryLogicIssue_ = {
				"Location" : "Attribute Primary Key",
				"issues" : AttributePrimaryLogicIndex
			}
			allIssues.append(_AttributePrimaryLogicIssue_)

'''
	Check the Attribute Uniqueness for the issues. Attribute Uniqueness should match with the values in template.json
	@Params : _attributeValue - Attribute Uniqueness
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	
def __attributeUniqueness(_attributeValue, count, dfs, _):
	location = findLoc(dfs,'Attribute Uniqueness',_)
	global _AttributeUniquenessIssue_
	if str(_attributeValue).lower() not in mappingJsonFile['attributeBool']:
		if str(_attributeValue).lower() == 'nan':
			_attributeValue = '*This field is mandatory'
		issue = {
				"type":"ERROR",
				"issueValue":'The issue in the Attribute Uniqueness',
				"expectedValue":"Attribute Uniqueness should be either Yes or No",
				"actualValue":_attributeValue,
				"location":location,
				"issueDesc":"NA will be consider as No value for Mandatory Field and it is mandatory to fill the Attribute Uniqueness. "
			}
		AttributeUniquenessIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_AttributeUniquenessIssue_ = {}
	if count['index'] == count['count']:
		_AttributeUniquenessIssue_ = {
			"Location" : "Attribute Uniqueness",
			"issues" : AttributeUniquenessIndex
		}
		if bool(_AttributeUniquenessIssue_['issues']):
			allIssues.append(_AttributeUniquenessIssue_)

'''
	Check the Attribute Description for the issues.
	@Params : _attribute - Attribute Description
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
'''
def __attributeDescription(_attribute, count, dfs, _):
	location = findLoc(dfs,'Attribute Description',_)
	global _AttributeDescriptionIssue_ 
	if not re.findall("[^A-Za-z0-9_()?:;,.…'></\n -]", str(_attribute)):
		_AttributeDescriptionIssue_ = {}
		pass;
	else:
		issue = {
				"type":"ERROR",
				"issueValue":'The issue in the Attribute Description',
				"expectedValue":"Special Characters are not allowed like % $ ^ ' \" allowed Characters are 0-9,a-z,A-z,() and Underscore.",
				"actualValue":_attribute,
				"location":location,
				"issueDesc":"The issue will be with Attribute Description. Special Characters are not allowed like % $ ^ ' \" allowed Characters are 0-9,a-z,A-z,() and Underscore."
			}
		AttributeDescriptionIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
	_AttributeDescriptionIssue_ = {}
	if count['index'] == count['count']:
		_AttributeDescriptionIssue_ = {
			"Location" : "Attribute Description",
			"issues" : AttributeDescriptionIndex
		}
		if bool(_AttributeDescriptionIssue_['issues']):
			allIssues.append(_AttributeDescriptionIssue_)

'''
'''
	Check the language for the issues. Language should match with the values in template.json
	@Params : _language - Language
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	

def __language(_language, count, dfs, _):
	location = findLoc(dfs,'Language',_)
	global _LanguageIssue_
	if str(_language).lower() not in mappingJsonFile['language'] and str(_language).lower() != 'nan':
		issue = {
				"type":"ERROR",
				"issueValue":'The issue in the Language',
				"expectedValue":"Language should be either English,Arabic,Mix or Other",
				"actualValue":_language,
				"location":location,
				"issueDesc":"Language should be either English,Arabic,Mix or Other. Empty column or NA will be consider as No value."
			}
		LanguageIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_LanguageIssue_ = {}
	if count['index'] == count['count']:
		_LanguageIssue_ = {
			"Location" : "Language",
			"issues" : LanguageIndex
		}
		if bool(_LanguageIssue_['issues']):
			allIssues.append(_LanguageIssue_)
	
'''
	Check the Attribute Classification for the issues. Attribute Classification should match with the values in template.json
	@Params : _classification - Attribute Classification
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	

def __classification(_classification, count, dfs, _):
	location = findLoc(dfs,'Attribute Classification',_)
	global _ClassificationIssue_
	if str(_classification).lower() not in mappingJsonFile['classification']:
		if str(_classification).lower() == 'nan':
			_classification = '*This field is mandatory'
		issue = {
				"type":"ERROR",
				"issueValue":'The issue in the Attribute Classification',
				"expectedValue":"Attribute Classification should be either Open, Confidential, Sensitive",
				"actualValue":_classification,
				"location":location,
				"issueDesc":"NA will be consider as No value for Mandatory Field and it is mandatory to fill the Attribute Classification. "
			}
		ClassificationIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_ClassificationIssue_ = {}
	if count['index'] == count['count']:
		_ClassificationIssue_ = {
			"Location" : "Attribute Classification",
			"issues" : ClassificationIndex
		}
		if bool(_ClassificationIssue_['issues']):
			allIssues.append(_ClassificationIssue_)

'''
	Check the Data Types for the issues.
	@Params : _dataType - Data Type
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''

def __dataType(_dataType, count, dfs, _):
	location = findLoc(dfs,'Attribute DataType',_)
	global _DataTypeIssue_
	if str(_dataType).lower() not in mappingJsonFile['dataTypes']:
		if str(_dataType).lower() == 'nan':
			_dataType = '*This field is mandatory'
		issue = {
				"type":"ERROR",
				"issueValue":'The issue in the Data Type',
				"expectedValue":"Data Type should be match with " + listToComma(mappingJsonFile['dataTypes'] ),
				"actualValue":_dataType,
				"location":location,
				"issueDesc":"Data Type should be match with expected Value. Don't add (size) in Data Type. There is seperate column for Size."
			}
		DataTypeIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_DataTypeIssue_ = {}
	if count['index'] == count['count']:
		_DataTypeIssue_ = {
			"Location" : "Data Type",
			"issues" : DataTypeIndex
		}
		if bool(_DataTypeIssue_['issues']):
			allIssues.append(_DataTypeIssue_)

'''
	Check the Size of Data Types for the issues.
	@Params : _dataTypeSize - Data Type Size
			  _dataType - Data Type at current Cursor.
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __dataTypeSize(_dataTypeSize,_dataType, count, dfs, _):
	location = findLoc(dfs,'Attribute Size',_)
	global _DataTypeSizeIssue_
	if str(_dataTypeSize).lower() == 'nan':
		if re.match("date|time", str(_dataType).lower()):
			pass;
		elif str(_dataType).lower() in mappingJsonFile['numberDataTypeSize']:
			pass;
		else:

			_dataTypeSize = '*This field is mandatory'
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Data Type Size',
					"expectedValue":"Size should be available for all Data Types except date,datetime. DataType is: "+str(_dataType)+" but the size is empty.",
					"actualValue":_dataTypeSize,
					"location":location,
					"issueDesc":"Size should be available for all Data Types except date,datetime."
				}
			DataTypeSizeIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
	else:
		_dataTypeSize = str(_dataTypeSize).split('.')[0]
		if re.match("^[0-9,]*$", str(_dataTypeSize)):
			if ',' in str(_dataTypeSize):
				if str(_dataTypeSize).split(',')[0] == '0':
					issue = {
							"type":"ERROR",
							"issueValue":'The issue in the Data Type Size',
							"expectedValue":"Precision cannot be zero(0)",
							"actualValue":str(_dataTypeSize),
							"location":location,
							"issueDesc":"The issue in the Data Type Size. Size should be numeric only or can have comma in case of float."
						}
					DataTypeSizeIndex.append(issue)
					log.error(issue['issueValue'],extra=issue) 
			if str(_dataTypeSize).strip() == '0':
				issue = {
						"type":"ERROR",
						"issueValue":'The issue in the Data Type Size',
						"expectedValue":"Precision cannot be zero(0)",
						"actualValue":str(_dataTypeSize),
						"location":location,
						"issueDesc":"The issue in the Data Type Size. Size should be numeric only or can have comma in case of float."
					}
				DataTypeSizeIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
		else:
			_dataTypeSize = 'Only Number and comma is allowed'
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Data Type Size',
					"expectedValue":"Only Number and comma is allowed. DataType is: "+str(_dataType)+" having alphabets or any other character",
					"actualValue":_dataTypeSize,
					"location":location,
					"issueDesc":"The issue in the Data Type Size. Size should be numeric only or can have comma in case of float."
				}
			DataTypeSizeIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
	_DataTypeSizeIssue_ = {}
	if count['index'] == count['count']:
		_DataTypeSizeIssue_ = {
			"Location" : "DataType Size",
			"issues" : DataTypeSizeIndex
		}
		if bool(_DataTypeSizeIssue_['issues']):
			allIssues.append(_DataTypeSizeIssue_)

'''
	Check the Size of Date/Time Format for the issues.
	@Params : _attributeRange - Attribute Range/Date Format
			  _dataType - Data Type at current Cursor.
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __dateFormat(_attributeRange,_dataType, count, dfs, _):
	location = findLoc(dfs,'Attribute Range of Values',_)
	global _DateFormatIssue_
	if re.match("date|time", str(_dataType).lower()):
		if str(_attributeRange).lower() == 'nan' or str(_attributeRange).upper() not in mappingJsonFile['dateformat']:
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Date/Time Format',
					"expectedValue":"Date/Time Format should be one of UDF_TO_DATETIME valid formats. ",
					"actualValue":_attributeRange,
					"location":location,
					"issueDesc":"Date/Time Format not matching one of the valid formats from UDF_TO_DATETIME"
			}
			DateTimeFormatIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
		else:
			pass;
	_DateFormatIssue_ = {}
	if count['index'] == count['count']:
		_DateFormatIssue_ = {
			"Location" : "DateTime Format",
			"issues" : DateTimeFormatIndex
		}
		if bool(_DateFormatIssue_['issues']):
			allIssues.append(_DateFormatIssue_)

'''
	Check the Connectivity for the issues.
	@Params : _connectivity - Connectivity
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __connectivity(_connectivity, count, dfs, _):
	location = findLoc(dfs,'Connectivity Option',_)
	global _ConnectivityIssue_
	if str(_connectivity).lower() not in map(lambda x: x.lower(), mappingJsonFile['connectivity']) :
		if str(_connectivity).lower() == 'nan':
			pass;
		else:
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Connectivity Option.',
					"expectedValue":"Connectivity should be match with " + listToComma(mappingJsonFile['connectivity'] ),
					"actualValue":_connectivity,
					"location":location,
					"issueDesc":"Connectivity should be match with expected values."
				}
			ConnectivityIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_ConnectivityIssue_ = {}
	if count['index'] == count['count']:
		_ConnectivityIssue_ = {
			"Location" : "Connectivity Issue",
			"issues" : ConnectivityIndex
		}
		if bool(_ConnectivityIssue_['issues']):
			allIssues.append(_ConnectivityIssue_)

'''
	Check the Connectivity Description for the issues.
	@Params : _connectivityDesc - Connectivity Description
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __connectivityDesc(_connectivityDesc, count, dfs, _):
	Connectivity_TYPE.append(_connectivityDesc)
	location = findLoc(dfs,'Description for Connectivity',_)
	global _ConnectivityDescIssue_
	if str(_connectivityDesc).lower() not in map(lambda x: x.lower(), mappingJsonFile['connectivityDesc']) :
		if str(_connectivityDesc).lower() == 'nan':
			if True in list(map(lambda x:str(x) != 'nan',Connectivity_TYPE)):
				pass
			else:
				issue = {
						"type":"ERROR",
						"issueValue":'The issue in the Connectivity Description.',
						"expectedValue":"Connectivity Description should be match with " + listToComma(mappingJsonFile['connectivityDesc'] ),
						"actualValue":_connectivityDesc,
						"location":location,
						"issueDesc":"Connectivity Description should be match with expected values."
					}
				ConnectivityDescIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
		else:
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Connectivity Description.',
					"expectedValue":"Connectivity Description should be match with " + listToComma(mappingJsonFile['connectivityDesc'] ),
					"actualValue":_connectivityDesc,
					"location":location,
					"issueDesc":"Connectivity Description should be match with expected values."
				}
			ConnectivityDescIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
	else:
		pass
	_ConnectivityDescIssue_ = {}
	if count['index'] == count['count']:
		_ConnectivityDescIssue_ = {
			"Location" : "Connectivity Description Issue",
			"issues" : ConnectivityDescIndex
		}
		if bool(_ConnectivityDescIssue_['issues']):
			allIssues.append(_ConnectivityDescIssue_)

'''
	Check the Encoding of Dataset.
	@Params : _encoding - Encoding
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __checkEncoding(_encoding, count, dfs, _):
	ENCODING.append(_encoding)
	location = findLoc(dfs,'Code Page',_)
	global _EncodingIssue_
	if True in list(map(lambda x:str(x) != 'nan',ENCODING)):
		pass
	else:

		if str(_encoding).lower() != 'utf-8':
			if str(_encoding).lower() == 'nan':
				issue = {
						"type":"ERROR",
						"issueValue":'The issue in the Encoding.',
						"expectedValue":"Encoding is mandatory. If encoding exist in seperate column, merge the encoding columns",
						"actualValue":_encoding,
						"location":location,
						"issueDesc":"Encoding Column should be merged for the Dataset, it should be a single value."
					}
				EncodingIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
			else:
				issue = {
						"type":"ERROR",
						"issueValue":'The issue in the Encoding.',
						"expectedValue":"Encoding should be UTF-8",
						"actualValue":_encoding,
						"location":location,
						"issueDesc":"Code Page Column shoud be UTF-8 without Spaces."
					}
				EncodingIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
		else:
			pass
		_EncodingIssue_ = {}
		if count['index'] == count['count']:
			_EncodingIssue_ = {
				"Location" : "Encoding Issue",
				"issues" : EncodingIndex
			}
			if bool(_EncodingIssue_['issues']):
				allIssues.append(_EncodingIssue_)

'''
	Check the Delimeter of Dataset from Data Contract.
	@Params : _delimiter - Delimiter
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __checkDelimiter(_delimiter, _delimiterother, count, dfs, _):
	DELIMITER.append(_delimiter)
	location = findLoc(dfs,'Attribute Delimiter',_)
	global _DelimiterIssue_
	if True in list(map(lambda x:str(x) != 'nan',DELIMITER)):
		if str(_delimiter) != 'nan':
			if str(_delimiter) != 'Other':
				DELIMITER_VALUE.append(str(_delimiter))
			else:
				DELIMITER_VALUE.append(str(_delimiterother))
		pass
	else:
		if str(_delimiter).lower() == 'nan':
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Delimiter.',
					"expectedValue":"Delimiter is mandatory. If Delimiter exist in seperate column, merge the Delimiter columns",
					"actualValue":_delimiter,
					"location":location,
					"issueDesc":"Delimiter Column should be merged for the Dataset, it should be a single value."
				}
			DelimiterIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
		else:
			pass
		_DelimiterIssue_ = {}
		if count['index'] == count['count']:
			_DelimiterIssue_ = {
				"Location" : "Delimiter Issue",
				"issues" : DelimiterIndex
			}
			if bool(_DelimiterIssue_['issues']):
				allIssues.append(_DelimiterIssue_)

'''
	Check the Service for the issues.
	@Params : _service - Service
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __checkService(_service, count, dfs, _):
	SERVICE.append(_service)
	location = findLoc(dfs,'Service',_)
	global _ServiceIssue_
	if True in list(map(lambda x:str(x) != 'nan',SERVICE)):
		pass
	else:
		if str(_service).lower() == 'nan':
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Service.',
					"expectedValue":"Service is mandatory. If encoding exist in seperate column, merge the service columns",
					"actualValue":_service,
					"location":location,
					"issueDesc":"Service Column should be merged for the Dataset, it should be a single value."
				}
			ServiceIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
		_ServiceIssue_ = {}
		if count['index'] == count['count']:
			_ServiceIssue_ = {
				"Location" : "Service Issue",
				"issues" : ServiceIndex
			}
			if bool(_ServiceIssue_['issues']):
				allIssues.append(_ServiceIssue_)

'''
	Check the Category for the issues.
	@Params : _category - Category
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __checkCategory(_category, count, dfs, _):
	CATEGORY.append(_category)
	location = findLoc(dfs,'Category',_)
	global _CategoryIssue_
	if True in list(map(lambda x:str(x) != 'nan',CATEGORY)):
		pass
	else:

		if str(_category).lower() == 'nan':
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Category.',
					"expectedValue":"Category is mandatory. If encoding exist in seperate column, merge the Category columns",
					"actualValue":_category,
					"location":location,
					"issueDesc":"Category Column should be merged for the Dataset, it should be a single value."
				}
			CategoryIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
		_CategoryIssue_ = {}
		if count['index'] == count['count']:
			_CategoryIssue_ = {
				"Location" : "Category Issue",
				"issues" : CategoryIndex
			}
			if bool(_CategoryIssue_['issues']):
				allIssues.append(_CategoryIssue_)

'''
	Check the Entity for the issues.
	@Params : _entity - Entity
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __checkEntity(_entity, count, dfs, _):
	ENTITYDC.append(_entity)
	location = findLoc(dfs,'Entity',_)
	global _EntityIssue_
	if True in list(map(lambda x:str(x) != 'nan',ENTITYDC)):
		pass
	else:
		if str(_entity).lower() == 'nan':
			issue = {
					"type":"ERROR",
					"issueValue":'The issue in the Entity.',
					"expectedValue":"Entity is mandatory. If encoding exist in seperate column, merge the entity columns",
					"actualValue":_entity,
					"location":location,
					"issueDesc":"Entity Column should be merged for the Dataset, it should be a single value."
				}
			EntityIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
		_EntityIssue_ = {}
		if count['index'] == count['count']:
			_EntityIssue_ = {
				"Location" : "Entity Issue",
				"issues" : EntityIndex
			}
			if bool(_EntityIssue_['issues']):
				allIssues.append(_EntityIssue_)

'''
	Check the Classification
	@Params : _classification - Classification
			  count - As it is the value of column for the xth row so need to find the end, thats why count is required.
			  dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''
def __checkClassification(_classification, count, dfs, _,attributeClassificationList):
	_attributeClassificationList = []
	DATASET_CLASSIFICATION.append(_classification)
	location = findLoc(dfs,'Data Classification Type',_)
	global _DatasetClassificationIssue_
	if str(_classification).lower() not in map(lambda x: x.lower(), mappingJsonFile['classification']) :
		if str(_classification).lower() == 'nan':
			if True in list(map(lambda x:str(x) != 'nan',DATASET_CLASSIFICATION)):
				pass
			else:
				issue = {
						"type":"ERROR",
						"issueValue":'The issue in the Classification.',
						"expectedValue":"Classification should be match with " + listToComma(mappingJsonFile['classification'] ),
						"actualValue":_classification,
						"location":location,
						"issueDesc":"Classification should be match with expected values."
					}
				DatasetClassificationIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
		else:
			issue = {
						"type":"ERROR",
						"issueValue":'The issue in the Classification.',
						"expectedValue":"Classification should be match with " + listToComma(mappingJsonFile['classification'] ),
						"actualValue":_classification,
						"location":location,
						"issueDesc":"Classification should be match with expected values."
					}
			DatasetClassificationIndex.append(issue)
			log.error(issue['issueValue'],extra=issue)
	else:
		for attr in attributeClassificationList:
			_attributeClassificationList.append(attr.lower().strip())
		if 'sensitive' in _attributeClassificationList:
			if _classification.lower() != 'sensitive':
				issue = {
							"type":"ERROR",
							"issueValue":'The issue in the Classification.',
							"expectedValue":"Classification is not matching with the attribute level classification",
							"actualValue":_classification,
							"location":location,
							"issueDesc":"Classification should be match with expected values."
						}
				DatasetClassificationIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
		elif 'confidential' in _attributeClassificationList and 'sensitive' not in _attributeClassificationList:
			if _classification.lower().strip() != 'confidential':
				issue = {
							"type":"ERROR",
							"issueValue":'The issue in the Classification.',
							"expectedValue":"Classification is not matching with the attribute level classification",
							"actualValue":_classification,
							"location":location,
							"issueDesc":"Classification should be match with expected values."
						}
				DatasetClassificationIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
	_DatasetClassificationIssue_ = {}
	if count['index'] == count['count']:
		_DatasetClassificationIssue_ = {
			"Location" : "Dataset Classification Issue",
			"issues" : DatasetClassificationIndex
		}
		if bool(_DatasetClassificationIssue_['issues']):
			allIssues.append(_DatasetClassificationIssue_)

'''
	Use : Postgres Connectivity
	Desc : COnnect to postgres to fetch Entities.
'''
def connect_to_postgres(psql_db, psql_user, psql_password, psql_host, psql_port):
	# Connect to PostgreSQL database using credentials from config file.  
	try:
		db_params = {
			'dbname': psql_db,
			'user': psql_user,
			'password': psql_password,
			'host': psql_host,
			'port': psql_port
			}

		connection = psycopg2.connect(**db_params)
		cursor = connection.cursor()
		log.info("Successfully connected to the database")
		return connection, cursor
	except Exception as error:
		log.error(f"Error connecting to PostgreSQL: {error}")
		return None, None



'''
	Parse the data contract and pass it to all the checking functions
	@Params : dfs - Data Frame - Required to find the location of the issue.
			  _ - _ is the row Number for the current cursor.
	return : Append to Issue Block
'''	
def parseDatasets(dfs):
	dfs = dfs.dropna(how='all')
	##dfs[['Dataset Name']] = dfs[['Dataset Name']].fillna(method='ffill')
	##dfs = dfs.fillna(method='ffill')
	dfs['Dataset Name'] = dfs['Dataset Name'].ffill()
	dfs = dfs.ffill()
	datasets = dfs['Dataset Name'].dropna().unique()
	_dataset = []
	_loc = dfs.columns.get_loc('Attribute')
	_classificationLoc = dfs.columns.get_loc('Attribute Classification')
	for index, dataset in enumerate(datasets):
		_dfs = dfs.loc[dfs['Dataset Name'] == dataset]
		_dataset_ = camelCase(strippedText(dataset))
		cleanIndexes()
		count = len(_dfs)
		_index_ = 0
		_count_ = {
			"count" : count,
			"index": _index_
		}
		INGESTION_TYPE = []
		Connectivity_TYPE = []
		DATASET_CLASSIFICATION = []
		PRIMARY_KEY = []

		_attributeExecuted  = False
		
		for idx, (_, row) in enumerate(_dfs.iterrows()):
			
			if _attributeExecuted == False:
				_indexRange = list(dfs.loc[dfs['Dataset Name']==dataset].index)
				_start  = _indexRange[0]
				_end = _indexRange[-1] + 1
				_attributeList = dfs.values[_start:_end,_loc]
				_attributeClassificationList = dfs.values[_start:_end,_classificationLoc]
				_attributeExecuted = True
			
			_index_ = _index_+ 1
			_count_["index"] = _index_
			__ingestionType(row['Ingestion Logic'], _count_, dfs, _)	
			__dataset(row['Dataset Name'], _count_, dfs, _)   #------ do we want to check the length of the dataset name??
			__attribute(row['Attribute'], _count_, dfs, _)
			# #143 suggested that special characters should be allowed as description is free text so no check is required for Description
			# Commenting the Function Defination if required in later stage.
			# __attributeDescription(row['Attribute Description'], _count_, dfs, _)
			__attributeNullability(row['Attribute Nullability'], _count_, dfs, _)
			__attributePrimaryKey(row['Attribute Primary Key'], _count_, dfs, _)
			__attributeUniqueness(row['Attribute Uniqueness'], _count_, dfs, _)
			__format(row['Format (MIME)'], _count_, dfs, _)
			__splitLogic(row['Split Logic'], _count_, dfs, _)
			__dataContractType(row['DataContract Type'], _count_, dfs, _)
			__frequency(row['Frequency of Update on Source'], _count_, dfs, 'Frequency of Update on Source' , _)
			__frequency(row['Frequency of Update to SDP'], _count_, dfs, 'Frequency of Update to SDP' , _)
			__language(row['Language'], _count_, dfs, _)  #--------- not in & !=nan should be not in or !=nan
			__classification(row['Attribute Classification'], _count_, dfs, _)
			__dataType(row['Attribute DataType'], _count_, dfs, _)
			__dataTypeSize(row['Attribute Size'],row['Attribute DataType'], _count_, dfs, _)
			__dateFormat(row['Attribute Range of Values'],row['Attribute DataType'], _count_, dfs, _)
			__connectivity(row['Connectivity Option'], _count_, dfs, _)
			__connectivityDesc(row['Description for Connectivity'], _count_, dfs, _)
			__checkEncoding(row['Code Page'], _count_, dfs, _)
			__checkDelimiter(row['Attribute Delimiter'], row['Attribute Delimiter- Other'], _count_, dfs, _)
			__checkService(row['Service'], _count_, dfs, _)
			__checkCategory(row['Category'], _count_, dfs, _)
			__checkEntity(row['Entity'], _count_, dfs, _)
			__checkClassification(row['Data Classification Type'], _count_, dfs, _,_attributeClassificationList)
		parseSampleFile(_dataset_,_attributeList)	
		if len(allIssues) > 0:
			mainIssue = {
				"DatasetName" : dataset,
				"allIssue" : allIssues
			}
			Issues.append(mainIssue)
		_dataset.append(_dataset_)
	return dfs,_dataset

'''
	Parse Sample Files
	@Params : datasetName - Dataset Name
			  attributes - Attribute List to Match with Header of Sample Files
	return : Append to Issue Block
'''
def parseSampleFile(datasetName,attributes):
	datasetName = _entityName+'_'+datasetName
	global _SampleFileIssue_
	if os.path.isdir(data_contract+'/sampleFiles') == False:
		return
	for files in os.listdir(data_contract+'/sampleFiles'):
		file = tuple(item for item in files.split('.'))
		FILES.append(file)
	files = dict(FILES)
	# print(datasetName, files)
	if datasetName in files:
		try:
			if files[datasetName] == 'csv':
				enc = predict_encoding(data_contract+ "sampleFiles/"+datasetName+'.csv',5)
			elif files[datasetName] == 'txt':
				enc = predict_encoding(data_contract+ "sampleFiles/"+datasetName+'.txt',5)	
			elif files[datasetName] == 'kml':
				enc = predict_encoding(data_contract+ "sampleFiles/"+datasetName+'.kml',5)
			if(str(enc).lower() != 'utf-8' and str(enc).lower() != 'ascii'):
				issue = {
						"type":"ERROR",
						"issueValue":'The Sample File is not in UTF-8 Format' ,
						"expectedValue":"The Sample File for dataset "+ str(datasetName) +" need to be in UTF-8 format. ",
						"actualValue":"Format is Sample File is: " + str(enc).upper(),
						"location":"Sample File " + str(datasetName),
						"issueDesc":"The Sample File is not in UTF-8 Format"
				}
				if len(SampleFileIndex) > 0:
					pass;
				else:
					SampleFileIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
				_SampleFileIssue_ = {}
				
				_SampleFileIssue_ = {
					"Location" : "Sample Files",
					"issues" : SampleFileIndex
				}
				if bool(_SampleFileIssue_['issues']):
					allIssues.append(_SampleFileIssue_)
			else:
				pass
			if files[datasetName] == 'csv':
				file = data_contract+ "sampleFiles/"+datasetName+'.csv'
				with open(file, 'r',  encoding="utf8") as f1:
					dialect = csv.Sniffer().sniff(f1.read()) 
					delimiter= str(dialect.delimiter)
				_reader = pd.read_csv(data_contract+ "sampleFiles/"+datasetName+'.csv', sep=delimiter, header=[0,0], dtype='unicode')
				sampleCount = len(_reader.index)
				if DELIMITER_VALUE[0].strip().lower() == 'tab':
					DELIMITER_VALUE[0] = '\t'
				if delimiter != DELIMITER_VALUE[0]:
					issue = {
							"type":"ERROR",
							"issueValue":'The Sample File Delimiter is '+str(delimiter) +" and Data Contract delimeter is "+ DELIMITER_VALUE[0],
							"expectedValue":"The Sample File delimiter should Match with the Data Contract.",
							"actualValue":str(DELIMITER_VALUE[0]),
							"location":"Sample File " + str(datasetName),
							"issueDesc":"The Sample File delimiter should Match with the Data Contract."
					}
					if len(SampleFileIndex) > 0:
						pass;
					else:
						SampleFileIndex.append(issue)
					log.error(issue['issueValue'],extra=issue)
					_SampleFileIssue_ = {}
					
					_SampleFileIssue_ = {
						"Location" : "Sample Files",
						"issues" : SampleFileIndex
					}
					if bool(_SampleFileIssue_['issues']):
						allIssues.append(_SampleFileIssue_)
				HEADER = _reader.columns.get_level_values(0).tolist()
				attributes = [attr.strip().upper() for attr in attributes]
				HEADER = [head.strip().upper() for head in HEADER]
				sampleFileColumns = ','.join(HEADER)
				dataContractColumn = ','.join(attributes)
				if sampleFileColumns != dataContractColumn:
					issue = {
							"type":"ERROR",
							"issueValue":'The Sample File Sequence Doesn\'t Match with the Data Contract Attribute' ,
							"expectedValue":"The Sample File Sequence should Match with the Data Contract Attribute. The sequence Should be " + dataContractColumn,
							"actualValue":sampleFileColumns,
							"location":"Sample File " + str(datasetName),
							"issueDesc":"The Sample File Sequence Doesn\'t Match with the Data Contract Attribute"
					}
					if len(SampleFileIndex) > 0:
						pass;
					else:
						SampleFileIndex.append(issue)
					log.error(issue['issueValue'],extra=issue)
					_SampleFileIssue_ = {}
					
					_SampleFileIssue_ = {
						"Location" : "Sample Files",
						"issues" : SampleFileIndex
					}
					if bool(_SampleFileIssue_['issues']):
						allIssues.append(_SampleFileIssue_)
			elif files[datasetName] == 'txt':
				try:
					file = data_contract+ "sampleFiles/"+datasetName+'.txt'
					with open(file, 'r',  encoding="utf8") as f1:
						dialect = csv.Sniffer().sniff(f1.read()) 
						delimiter= str(dialect.delimiter)
					reader = pd.read_csv(data_contract+ "sampleFiles/"+datasetName+'.txt', sep=delimiter, header=[0,0])
					sampleCount = len(reader.index)
					if DELIMITER_VALUE[0].strip().lower() == 'tab':
						DELIMITER_VALUE[0] = '\t'
					if delimiter != DELIMITER_VALUE[0]:
						issue = {
								"type":"ERROR",
								"issueValue":'The Sample File Delimiter is '+str(delimiter) +" and Data Contract delimeter is "+ DELIMITER_VALUE[0],
								"expectedValue":"The Sample File delimiter should Match with the Data Contract.",
								"actualValue":str(DELIMITER_VALUE[0]),
								"location":"Sample File " + str(datasetName),
								"issueDesc":"The Sample File delimiter should Match with the Data Contract."
						}
						if len(SampleFileIndex) > 0:
							pass;
						else:
							SampleFileIndex.append(issue)
						log.error(issue['issueValue'],extra=issue)
						_SampleFileIssue_ = {}
						
						_SampleFileIssue_ = {
							"Location" : "Sample Files",
							"issues" : SampleFileIndex
						}
						if bool(_SampleFileIssue_['issues']):
							allIssues.append(_SampleFileIssue_)
					HEADER = reader.columns.get_level_values(0).tolist()
					attributes = [attr.strip().upper() for attr in attributes]
					HEADER = [head.strip().upper() for head in HEADER]
					sampleFileColumns = ','.join(HEADER)
					dataContractColumn = ','.join(attributes)
					if sampleFileColumns != dataContractColumn:
						issue = {
							"type":"ERROR",
							"issueValue":'The Sample File Sequence Doesn\'t Match with the Data Contract Attribute' ,
							"expectedValue":"The Sample File Sequence should Match with the Data Contract Attribute. The sequence Should be " + dataContractColumn,
							"actualValue":sampleFileColumns,
							"location":"Sample File " + str(datasetName),
							"issueDesc":"The Sample File Sequence Doesn\'t Match with the Data Contract Attribute"
					}
						if len(SampleFileIndex) > 0:
							pass;
						else:
							SampleFileIndex.append(issue)
						log.error(issue['issueValue'],extra=issue)
						_SampleFileIssue_ = {}
						
						_SampleFileIssue_ = {
							"Location" : "Sample Files",
							"issues" : SampleFileIndex
						}
						if bool(_SampleFileIssue_['issues']):
							allIssues.append(_SampleFileIssue_)
				except:
					log.error("Exception occurred while finding delimeter of txt file", exc_info=True)
					raise
			else:
				pass
		except Exception as e:
			enc = predict_encoding(data_contract+ "sampleFiles/"+datasetName+'.csv',5)
			if(str(enc).lower() != 'utf-8' and str(enc).lower() != 'ascii'):
				issue = {
						"type":"ERROR",
						"issueValue":'The Sample File is not in UTF-8 Format' ,
						"expectedValue":"The Sample File for dataset "+ str(datasetName) +" need to be in UTF-8 format. ",
						"actualValue":"Format is Sample File is: " + str(enc).upper(),
						"location":"Sample File " + str(datasetName),
						"issueDesc":"The Sample File is not in UTF-8 Format"
				}
				if len(SampleFileIndex) > 0:
					pass;
				else:
					SampleFileIndex.append(issue)
				log.error(issue['issueValue'],extra=issue)
				_SampleFileIssue_ = {}
				
				_SampleFileIssue_ = {
					"Location" : "Sample Files",
					"issues" : SampleFileIndex
				}
				if bool(_SampleFileIssue_['issues']):
					allIssues.append(_SampleFileIssue_)
			else:
				pass
	else:
		issue = {
				"type":"ERROR",
				"issueValue":'The Sample File Doesn\'t exist for dataset '+ str(datasetName) ,
				"expectedValue":"The Sample File with the name "+ str(datasetName) + " should exist in sampleFiles Directory",
				"actualValue":"No Sample File Exist",
				"location":"Sample File " + str(datasetName),
				"issueDesc":"The Sample File Sequence Doesn\'t Match with the Data Contract Attribute"
		}
		if len(SampleFileIndex) > 0:
			pass;
		else:
			SampleFileIndex.append(issue)
		log.error(issue['issueValue'],extra=issue)
		_SampleFileIssue_ = {}
		
		_SampleFileIssue_ = {
			"Location" : "Sample Files",
			"issues" : SampleFileIndex
		}
		if bool(_SampleFileIssue_['issues']):
			allIssues.append(_SampleFileIssue_)

'''
	Use : Find Encoding of File
	Desc : Function will predict the encoding of the file by using top 10 lines
	@Params : file_path - Type:String - Path of the File
			  n_lines - Type:Number - No of Lines
	returns : encoding
'''
def predict_encoding(file_path, n_lines=10):
	# Open the file as binary data
	with open(file_path, 'rb') as f:
		# Join binary lines for specified number of lines
		rawdata = b''.join([f.readline() for _ in range(n_lines)])
	return chardet.detect(rawdata)['encoding']

'''
	Use : Check Exit Status
	Desc : Exit the execution if the issue occured in Script
	@Params : None
	returns : None
'''
def exitSys():
	_Issues = json.loads(Issues)
	with open(CODES_DIR+'/issues/checkIssue.txt', 'rb') as f:
		data = f.readline()
	if data:
		log.error("All issues in the Data Contract", exc_info=True)
		end = time.time()
		print(end - start)
		sys.exit(1)


'''
	Use : Write Report to HTML
	Desc : Generate a HTML Report from the report.json file generated by generateReport Function
	@Params : None
	returns : None
'''	
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



'''
	Entry Point of the Program
'''
def main():
	try: 
		log.info('Check Data Contract Module: Started')
		file, data_contract_path, psql_db, psql_user, psql_password, psql_host, psql_port = readInputArgs()
		log.info('Data Contract Path: %s .Data Contract File %s', data_contract_path,file)
		files = file.split(',')
		global _entityName
		global data_contract
		
		for file in files:
			_entityName = entityName(file, psql_db, psql_user, psql_password, psql_host, psql_port)
			path = data_contract_path + _entityName + '/'
			data_contract = path
			log.info('Data Contract Path inside loop: %s .Entity Name %s', data_contract,_entityName)
			execute(file,path)
		exitSys()
	except Exception as e: 
		log.error("Check Data Contract Module: Error occurred")
		raise


if __name__ == '__main__':
	main()
	end = time.time()
	print(end - start)





