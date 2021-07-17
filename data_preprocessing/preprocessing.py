import numpy as np
import pandas as pd
from imblearn.over_sampling import SMOTE
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler

class preprocessing():
    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger_object=logger_object

    def remove_columns(self,columns,data):
        self.columns=columns
        self.data=data
        self.logger_object.log(self.file_object,'Entered the remove_columns mehtod of class preprocessing')
        try:
            self.useful_col=self.data.drop(labels=self.columns,axis=1)
            self.logger_object.log(self.file_object,"Successfully removed the mentioned columns.")
            self.logger_object.log(self.file_object,'Exiting the remove_columns method of class preprocessing')
            return self.useful_col
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception has occured.Error: %s'%e)
            self.logger_object.log(self.file_object,'Columns removal unsuccessful. Exiting the remove_columns method of class preprocessing')
            raise Exception

    def XY_split(self,data,label):
        self.logger_object.log(self.file_object,'Entered the XY_split mtod of class preprocessing')
        try:
            self.X=data.drop(labels=label,axis=1)
            self.y=data[label]
            self.logger_object.log(self.file_object,'Successfully splitted the data into X and Y')
            self.logger_object.log(self.file_object, 'Exiting the XY_split method of class preprocessing')
            return self.X,self.y
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception error : %s'%e)
            self.logger_object.log(self.file_object,'XY splitting of data unsuccessful. Exiting the XY_split method of class preprocessing')
            raise Exception

    def is_null_present(self,data):
        self.logger_object.log(self.file_object,'Exited the is_null_present method of class preprocessing')
        self.null_present=False

        try:
            self.null_count=data.isna().sum()
            for i in self.null_count:
                if i>1:
                    self.null_present=True
            if(self.null_present):
                dataframe_with_null=pd.DataFrame()
                dataframe_with_null['columns']=data.columns
                dataframe_with_null['missing values count']=np.asarray(data.isna().sum())
                dataframe_with_null.to_csv('preprocessing_data/null_values.csv')
            self.logger_object.log(self.file_object,'Succesfully created a dataframe with null value counts. Exited the is_null_present method of class preprocessing')
            return self.null_present
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception has occured : %s'%e)
            self.logger_object.log(self.file_object,'Exiting the is_null_present method of class preprocessing')
            raise Exception

    def impute_missing_values(self,data):
        self.logger_object.log(self.file_object,'Entered the impute_missing_values method of class preprocessing')
        self.data=data
        try:
            imputer=KNNImputer(n_neighbors=3)
            self.new_array=imputer.fit_transform(self.data)
            self.new_data=pd.DataFrame(self.new_array,columns=self.data.columns)
            self.logger_object.log(self.file_object,'Successfully imputed the missing values. Exiting the impute_missing_values method of class preprocessing')
            return self.new_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception has occurred: %s'%e)
            self.logger_object.log(self.file_object,'Imputing missing values unsuccessful. Exiting the impute_missing_values method of class preprocessing')
            raise e

    def get_columns_with_zero_std(self,data):
        self.logger_object.log(self.file_object,'Entered the get_columns_with_zero_std method of class preprocessing')
        self.data=data
        self.columns=data.columns
        self.data_desc=data.describe()
        self.col_to_drop=[]

        try:
            for col in self.col_to_drop:
                if(self.data_desc[col]['std']==0):
                    self.col_to_drop.append(col)
                self.logger_object.log(self.file_object,'Successfully searched for columns with zero standard deviation. Exiting the get_columns_with_zero_std method of class preprocessing')
            return self.col_to_drop

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception has occurred: %s' % e)
            self.logger_object.log(self.file_object,'Search for columns with zero standard deviation unsuccessful. Exiting the get_columns_with_zero_std method method of class preprocessing')
            raise e

    def scale_data(self,data):
        self.logger_object.log(self.file_object, 'Entered the scale_method method of class preprocessing')
        scaler=StandardScaler()
        num_features=data[["elevation", "aspect", "slope", "horizontal_distance_to_hydrology", "Vertical_Distance_To_Hydrology",
             "Horizontal_Distance_To_Roadways", "Horizontal_Distance_To_Fire_Points"]]
        cat_features=data.drop(num_features,axis=1)
        scaled_feat=scaler.fit_transform(num_features)
        num_features=pd.DataFrame(scaled_feat,columns=num_features.columns,index=num_features.index)
        final_data=pd.concat(num_features,cat_features)
        return final_data

    def enocdeCategoricalvalues(self, data):

        data["class"] = data["class"].map({"Lodgepole_Pine": 0, "Spruce_Fir": 1, "Douglas_fir": 2, "Krummholz": 3, "Ponderosa_Pine": 4, "Aspen": 5,"Cottonwood_Willow": 6})

        return data

    def handleImbalancedDataset(self,X,y):
        sample=SMOTE()
        X,y=sample.fit_resample(X, y)
        return X,y





