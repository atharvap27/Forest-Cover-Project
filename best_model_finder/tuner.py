from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score,accuracy_score

class model_finder:
    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger_object=logger_object
        self.clf=RandomForestClassifier()
        self.xgb=XGBClassifier(objective='binary:logistic')

    def get_best_params_for_RandomForest(self,train_x,train_y):
        self.logger_object.log(self.file_object,'Entered the get_best_params_for_RandomForest method of class tuner')
        try:
            self.param_grid={"n_estimators": [10,50,100,130],"criterion":['gini','entropy'],"max_depth":range(2, 4, 1),"max_features":['auto', 'log2']}
            self.grid=GridSearchCV(estimator=self.clf,param_grid=self.param_grid,cv=5,verbose=3,n_jobs=-1)
            self.grid.fit(train_x,train_y)
            self.criterion=self.grid.best_params_['criterion']
            self.n_estimators=self.grid.best_params_['n_estimators']
            self.max_depth=self.grid.best_params_['max_depth']
            self.max_features=self.grid.best_params_['max_features']

            self.clf=RandomForestClassifier(n_estimators=self.n_estimators,criterion=self.criterion,max_depth=self.max_depth,max_features=self.max_features)
            self.clf.fit(train_x,train_y)
            self.logger_object.log(self.file_object,'Random Forest best params: '+str(self.grid.best_params_)+'. Exited the get_best_params_for_random_forest method of the Model_Finder class')
            return self.clf

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_best_params_for_random_forest method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Random Forest Parameter tuning  failed. Exited the get_best_params_for_random_forest method of the Model_Finder class')
            raise Exception


    def get_best_params_for_xgboost(self,train_x,train_y):
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_xgboost method of class tuner')
        try:
            self.param_grid_xgboost={'learning_rate': [0.5, 0.1, 0.01, 0.001], 'max_depth': [3, 5, 10, 20], 'n_estimators': [10, 50, 100, 200]}
            self.grid= GridSearchCV(XGBClassifier(objective='multi:softprob'),self.param_grid_xgboost, verbose=3,cv=5,n_jobs=-1)
            self.grid.fit(train_x,train_y)
            self.learning_rate=self.grid.best_params_['learning_rate']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.xgb=XGBClassifier(learning_rate=self.learning_rate, max_depth=self.max_depth, n_estimators=self.n_estimators)
            self.xgb.fit(train_x,train_y)
            self.logger_object.log(self.file_object,'XGBoost best params: ' + str(self.grid.best_params_) + '. Exited the get_best_params_for_xgboost method of the Model_Finder class. Exiting the get_best_params_for_xgboost method of class tuner')
            return self.xgb

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in get_best_params_for_xgboost method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'XGBoost Parameter tuning  failed. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception

    def get_best_model(self,train_x,train_y,test_x,test_y):
        self.logger_object.log(self.file_object,'Entered the get_best_model method of class tuner. Finding the best model for you...')
        try:
            self.xgboost=self.get_best_params_for_xgboost(train_x,train_y)
            self.prediction_xgboost=self.xgboost.predict_proba(test_x)
            if len(test_y.unique())==1:
                self.xgboost_score=accuracy_score(test_y,self.prediction_xgboost)
                self.logger_object.log(self.file_object,'Accuracy for XGBoost:'+str(self.xgboost_score))
            else:
                self.xgboost_score=roc_auc_score(test_y,self.prediction_xgboost,multi_class='ovr')
                self.logger_object.log(self.file_object, 'ROC_AUC_score for XGBoost:' + str(self.xgboost_score))
            self.random_forest=self.get_best_params_for_RandomForest(train_x,train_y)
            self.prediction_random_forest=self.random_forest.predict_proba(test_x)
            if len(test_y.unique())==1:
                self.random_forest_score=accuracy_score(test_y,self.prediction_random_forest)
                self.logger_object.log(self.file_object,'Accuracy for Random forest:'+str(self.random_forest_score))
            else:
                self.random_forest_score=roc_auc_score(test_y,self.prediction_random_forest,multi_class='ovr')
                self.logger_object.log(self.file_object, 'AUC for RF:' + str(self.random_forest_score))

            if (self.random_forest_score < self.xgboost_score):
                return 'XGBoost',self.xgboost
            else:
                return 'Random Forest',self.random_forest

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object, 'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()
