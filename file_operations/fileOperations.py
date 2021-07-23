import os
import pickle
import shutil

class File_Oerations:
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory_path = 'models/'
        self.model_name=None

    def save_model(self,model,file_name):
        self.logger_object.log(self.file_object,"The process of save model in class File_operations has been started")
        try:
            path = os.path.join(self.model_directory_path,file_name)
            if os.path.isdir(path):
                shutil.rmtree(self.model_directory_path)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path +'/'+file_name+'.sav','wb') as f:
                pickle.dump(model,f)
            self.logger_object.log(self.file_object,'Model saving has been successful!!')
            return 'success'
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in save model method of File_Operations class. Error is :'+str(e))
            self.logger_object.log(self.file_object,'Model File '+file_name+' could not be saved. Exited the save_model method of the File_Operations class')
            raise Exception()

    def load_model(self,file_name):
        self.logger_object.log(self.file_object,'Entered the load_model method of class File_Operations')
        try:
            with open(self.model_directory_path+file_name+'/'+file_name+'.sav','rb') as f:
                self.logger_object.log(self.file_object,'loaded the model from'+self.model_directory_path+file_name+'/'+file_name+'.sav')
                return pickle.load(f)
        except Exception as e:
            self.logger_object.log(self.file_object,'Error in loading the file from'+self.model_directory_path+file_name+'/'+file_name+'.sav')
            self.logger_object.log(self.file_object,'Exception error is:' +str(e))
            self.logger_object.log(self.file_object,'Exited from load_model method of class File_Operations')
            raise Exception()

    def find_correct_model_file(self,cluster_number):
        self.logger_object.log(self.file_object, 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number = cluster_number
            self.folder_name = self.model_directory_path
            self.list_of_model_files=[]
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:

                try:

                    if (self.file.index(str(self.cluster_number))!=-1):
                        self.model_name = self.file
                except:
                    continue
            self.model_name = self.model_name.split('.')[0]
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of class File_Operations')
            return self.model_name
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured in find_correct_model_file moethod of class FileOperation. Error is :'+str(e))
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of class File_Operations')
            raise Exception()
