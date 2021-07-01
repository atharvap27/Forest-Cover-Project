import pandas as pd

class Data_loader():
    def __init__(self,file_object,logging_object):
        self.filename='TrainingFile_from_db/Input_file.csv'
        self.file_object=file_object
        self.logging_object=logging_object

    def get_data(self):
        self.logging_object.log(self.file_object,'Entered the get_data method of Data_loader class')
        try:
            self.data=pd.read_csv(self.filename)
            self.logging_object.log(self.file_object,'Data loading successful!!Exiting the get_data method of class Data_loader')
            return self.data
        except Exception as e:
            self.logging_object.log(self.file_object,'Exception error as occured:%s'%e)
            self.logging_object.log(self.file_object,'Exitng the get_data method of class Data_loader')
            raise e
