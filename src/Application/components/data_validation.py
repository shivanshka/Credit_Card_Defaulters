import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from Application.application_logging.logger import App_Logger
from Application.components.dbOperation import dBOperation
from Application.components.data_transformation import DataTransformation
from Application.constants import *

class Raw_Data_validation:

    """
             This class shall be used for handling all the validation done on the Raw Training Data!!.

             Written By: Shivansh Kaushal
             Version: 1.0
             Revisions: None

             """

    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'schema_training.json'
        self.logger = App_Logger()


    def valuesFromSchema(self):
        """
            Method Name: valuesFromSchema
            Description: This method extracts all the relevant information from the pre-defined "Schema" file.
            Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
            On Failure: Raise ValueError,KeyError,Exception

                Written By: Shivansh Kaushal
            Version: 1.0
            Revisions: None

                    """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']
            
            file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","valuesfromSchemaValidationLog.txt"), 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()



        except ValueError:
            file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","valuesfromSchemaValidationLog.txt"), 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","valuesfromSchemaValidationLog.txt"), 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","valuesfromSchemaValidationLog.txt"), 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        """
            Method Name: manualRegexCreation
            Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                        This Regex is used to validate the filename of the training data.
            Output: Regex pattern
            On Failure: None

            Written By: Shivansh Kaushal
            Version: 1.0
            Revisions: None

                                        """
        # sample file name: "creditCardFraud_021119920_010222.csv"
        regex = "['creditCardFraud']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
            Method Name: createDirectoryForGoodBadRawData
            Description: This method creates directories to store the Good Data and Bad Data
                        after validating the training data.

            Output: None
            On Failure: OSError

            Written By: Shivansh Kaushal
            Version: 1.0
            Revisions: None

                    """

        try:
            path = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated", "Good_Raw")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated", "Bad_Raw")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","GeneralLog.txt"), 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):

        """
            Method Name: deleteExistingGoodDataTrainingFolder
            Description: This method deletes the directory made  to store the Good Data
                            after loading the data in the table. Once the good files are
                            loaded in the DB,deleting the directory ensures space optimization.
            Output: None
            On Failure: OSError

            Written By: Shivansh Kaushal
            Version: 1.0
            Revisions: None

            """

        try:
            path = os.path.join(ROOT_DIR,ARTIFACTS_DIR,'Training_Raw_files_validated')
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(os.path.join(path,'Good_Raw')):
                shutil.rmtree(os.path.join(path,'Good_Raw'))
                file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","GeneralLog.txt"), 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","GeneralLog.txt"), 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):

        """
            Method Name: deleteExistingBadDataTrainingFolder
            Description: This method deletes the directory made to store the bad Data.
            Output: None
            On Failure: OSError

            Written By: Shivansh Kaushal
            Version: 1.0
            Revisions: None

                    """

        try:
            path = os.path.join(ROOT_DIR,ARTIFACTS_DIR,'Training_Raw_files_validated')
            if os.path.isdir(os.path.join(path,'Bad_Raw')):
                shutil.rmtree(os.path.join(path,'Bad_Raw'))
                file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","GeneralLog.txt"), 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","GeneralLog.txt"), 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):

        """
            Method Name: moveBadFilesToArchiveBad
            Description: This method deletes the directory made  to store the Bad Data
                            after moving the data in an archive folder. We archive the bad
                            files to send them back to the client for invalid data issue.
            Output: None
            On Failure: OSError

                Written By: Shivansh Kaushal
            Version: 1.0
            Revisions: None

                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:

            source = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated", "Bad_Raw")
            if os.path.isdir(source):
                path = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"TrainingArchiveBadData")
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = os.path.join(path,f'BadData_{str(date)}_{str(time)}')
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(os.path.join(source,f), dest)
                file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","GeneralLog.txt"), 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated")
                if os.path.isdir(os.path.join(path,'Bad_Raw')):
                    shutil.rmtree(os.path.join(path,'Bad_Raw'))
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","GeneralLog.txt"), 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e




    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: Shivansh Kaushal
                    Version: 1.0
                    Revisions: None

                """


        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create new directories
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        good_dir = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated","Good_Raw")
        bad_dir = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated","Bad_Raw")
        try:
            f = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","nameValidationLog.txt"), 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy(os.path.join("Training_Batch_Files",filename), good_dir)
                            self.logger.log(f,"Valid File name!! File moved to Good Raw Folder :: %s" % filename)

                        else:
                            shutil.copy(os.path.join("Training_Batch_Files",filename), bad_dir)
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy(os.path.join("Training_Batch_Files",filename), bad_dir)
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy(os.path.join("Training_Batch_Files",filename), bad_dir)
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","nameValidationLog.txt"), 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e




    def validateColumnLength(self,NumberofColumns):
        """
        Method Name: validateColumnLength
        Description: This function validates the number of columns in the csv files.
                    It is should be same as given in the schema file.
                    If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                    If the column number matches, file is kept in Good Raw Data for processing.
                    The csv file is missing the first column name, this function changes the missing name to "creditCardFraud".
        Output: None
        On Failure: Exception

        Written By: Shivansh Kaushal
        Version: 1.0
        Revisions: None

        """
        try:
            f = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","columnValidationLog.txt"), 'a+')
            self.logger.log(f,"Column Length Validation Started!!")
            good_dir = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated","Good_Raw")
            bad_dir = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated","Bad_Raw")
            for file in listdir(good_dir):
                csv = pd.read_csv(os.path.join(good_dir,file))
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move(os.path.join(good_dir,file), bad_dir)
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","columnValidationLog.txt"), 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Logs","columnValidationLog.txt"), 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validateMissingValuesInWholeColumn(self):
        """
            Method Name: validateMissingValuesInWholeColumn
            Description: This function validates if any column in the csv file has all values missing.
                        If all the values are missing, the file is not suitable for processing.
                        SUch files are moved to bad raw data.
            Output: None
            On Failure: Exception

            Written By: Shivansh Kaushal
            Version: 1.0
            Revisions: None

                              """
        try:
            f = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Log","missingValuesInColumn.txt"), 'a+')
            self.logger.log(f,"Missing Values Validation Started!!")
            good_dir = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated","Good_Raw")
            bad_dir = os.path.join(ROOT_DIR,ARTIFACTS_DIR,"Training_Raw_files_validated","Bad_Raw")

            for file in listdir(good_dir):
                csv = pd.read_csv(os.path.join(good_dir,file))
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move(os.path.join(good_dir,file),bad_dir)
                        self.logger.log(f,"Invalid Column for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Credit"}, inplace=True)
                    csv.to_csv(os.path.join(good_dir,file), index=None, header=True)
        except OSError:
            f = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Log","missingValuesInColumn.txt"), 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open(os.path.join(ROOT_DIR,LOG_DIR,"Training_Log","missingValuesInColumn.txt"), 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)
        self.dataTransform = DataTransformation()
        self.dBOperation = dBOperation()
        self.log_file=os.path.join(ROOT_DIR,LOG_DIR,'Training_Main_Log.txt')
        self.file_object = open(self.log_file, 'a+')
        self.log_writer = App_Logger()

    def train_validation(self):
        try:
            self.log_writer.log(self.file_object, 'Start of Validation on files for Training')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object, "Starting Data Transforamtion!!")
            # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            self.log_writer.log(self.file_object, "DataTransformation Completed!!!")

            self.log_writer.log(self.file_object,
                                "Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb('Training', column_names)
            self.log_writer.log(self.file_object, "Table creation Completed!!")
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dBOperation.insertIntoTableGoodData('Training')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
            # export data in table to csvfile
            self.dBOperation.selectingDatafromtableintocsv('Training')
            self.file_object.close()

        except Exception as e:
            raise e