
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import seaborn as sns
import os
from statsmodels.stats.outliers_influence import variance_inflation_factor 
import pandas as pd
import numpy as np
from pyspark.sql.window import Window


# %matplotlib inline
from dateutil import parser
from sklearn.model_selection import train_test_split
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import window as w
import missingno as msno

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *
import sys
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import io
from outlier_plotting.sns import handle_outliers

import requests
from pyspark.sql import SQLContext
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from functools import reduce
from pyspark.sql import DataFrame





import logging
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

class EdaToolFunctions:
    # def getQuantileRange(self,sales_df):
    #     column=sales_df.columns
    #     for i in column:
    #         # print(i)
    #         changedTypedf = sales_df.withColumn(i, sales_df[i].cast(IntegerType()))
    #         # print(changedTypedf.printSchema())

    #         quantile = changedTypedf.approxQuantile([i], [0.25, 0.5, 0.75], 0)
    #         # print(quantile.count())
    #         if len(quantile[0]) :
    #             print("Quantile Value for column ", i )
    #             quantile_25 = quantile[0][0]
    #             quantile_50 = quantile[0][1]
    #             quantile_75 = quantile[0][2]
    #             print('quantile_25: '+str(quantile_25))
    #             print('quantile_50: '+str(quantile_50))
    #             print('quantile_75: '+str(quantile_75))

    def getMissingValueCount(self,sales_df ):
        columns=sales_df.columns
        for i in columns:

            print("Mising Value count for " ,i ,"is" , sales_df.filter(sales_df[i].isNull()).count())
        
        f, ax = plt.subplots(figsize=(11, 9))
        cmap = sns.cubehelix_palette(220, 10, as_cmap=True)
        sns.heatmap(sales_df.toPandas().isnull(), cmap=cmap)

        # here is the trick save your figure into a bytes object and you can afterwards expose it via flas
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png')
        bytes_image.seek(0)
        return bytes_image


    
        
    def covertDateintoMYD(self,sales_df,column):
        # col_name=input("enter the date column name which you want to convert into month day and year")
        df=sales_df.select(from_unixtime(unix_timestamp(column, 'dd.MM.yyyy')).alias('Date'))
        df=df.select(year("Date").alias('year'), month("Date").alias('month'), dayofmonth("Date").alias('day'))
        # sales_df= sales_df.toPandas().merge(df)
        df = df.withColumn("id", monotonically_increasing_id())
        sales_df = sales_df.withColumn("id", monotonically_increasing_id())
        sales_df=sales_df.join(df, "id", "inner").drop("id")

        print(sales_df.show())
        return sales_df

   
    def interquantileRange(self,df):
        
        bounds = {
        c: dict(
            zip(["q1","q2","q3"], df.approxQuantile(c, [0.25,0.50, 0.75], 0))
        )
        for c,d in zip(df.columns, df.dtypes) if d[1] in "int"
        }
        # print(bounds)
        quantile_25=[]
        quantile_50=[]
        quantile_75=[]
        iqr_list=[]
        c_list=[]
        min_list=[]
        max_list=[]
        for c in bounds:
            iqr = bounds[c]['q3'] - bounds[c]['q1']
           
            bounds[c]['min'] = bounds[c]['q1'] - (iqr * 1.5)
            bounds[c]['max'] = bounds[c]['q3'] + (iqr * 1.5)
            c_list.append(c)
            iqr_list.append(iqr)
            quantile_25.append(bounds[c]['q1'])
            quantile_50.append(bounds[c]['q2'])
            quantile_75.append(bounds[c]['q3'])
            min_list.append(bounds[c]['min'])
            max_list.append(bounds[c]['max'])

#             schema = StructType([
#             StructField('Category', StringType(), True),
#             StructField('Count', IntegerType(), True),
#             StructField('Description', StringType(), True)
# ])

        result=self.spark.createDataFrame(zip(c_list,iqr_list, quantile_25,quantile_50,quantile_75,min_list,max_list), schema=['columns','IQR', 'quantile_25','quantile_50','quantile_75','Min','Max'])
        
        return (bounds,result)

    def detectOutilers(self,df):
        bounds , result= self.interquantileRange(df)
        # outliers = {}
        # outlier_df=pd.DataFrame()
        outlier_df=df.select('date')

        print(bounds)
        for c in bounds:
            result_df = df.select(c, 
                *[
                    F.when(
                        ~df[c].between(bounds[c]['min'], bounds[c]['max']),
                        "yes"
                    ).otherwise("no").alias(c+'_outlier')
                ]
            )
            result_df=result_df.filter(result_df[c+'_outlier']!="no")
            print(result_df.toPandas().shape)
            print(result_df)
            result_df = result_df.withColumn("id", monotonically_increasing_id())
            outlier_df = outlier_df.withColumn("id", monotonically_increasing_id())
            outlier_df=outlier_df.join(result_df, "id", "inner").drop("id")
        # outlier_df=outlier_df.append(result_df)
        # outlier_df=outlier_df.join(result_df)
        print(outlier_df)
        
        return outlier_df

    def getCorrelationMatrix(self,sales_df):
        vector_col = "corr_features"
        print(sales_df.printSchema())
        columns=[]
        for c,d in zip(sales_df.columns, sales_df.dtypes) :
            if d[1] in ("int","double"):
                columns.append(c)
        print(columns)

        assembler = VectorAssembler(inputCols=columns, outputCol=vector_col)
        df_vector = assembler.transform(sales_df).select(vector_col)

        # get correlation matrix
        matrix = Correlation.corr(df_vector, vector_col)
        print(matrix.collect())
        

    def sumVisualiztion(self,sales_df,x_column,y_column):
        
        df_plot=sales_df.groupBy(x_column).sum(y_column)
        # window = Window.partitionBy(df_plot[x_column]).orderBy(df_plot["sum("+y_column+")"].desc())
        # df_plot=df_plot.select('*', rank().over(window).alias('rank')).filter(df_plot['rank'] <= 2) 

        df_plot=df_plot.orderBy("sum("+y_column+")", ascending=False).head(10)
        x=[]
        y=[]
        for i in df_plot:
            x.append(str(i.asDict()[x_column]))
            y.append(i.asDict()["sum("+y_column+")"])
        
        plt.bar(x,y)
        plt.xticks(rotation=90)
        plt.title("Distribution of "+ y_column+" over " +x_column)
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        # plt.show()
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png')
        bytes_image.seek(0)
        # plot_url = base64.b64encode(bytes_image.getvalue()).decode('utf8')
        return bytes_image

    def lineSumVisualiztion(self,sales_df,x_column,y_column):
        
        df_plot=sales_df.groupBy(x_column).sum(y_column)
        # window = Window.partitionBy(df_plot[x_column]).orderBy(df_plot["sum("+y_column+")"].desc())
        # df_plot=df_plot.select('*', rank().over(window).alias('rank')).filter(df_plot['rank'] <= 2) 

        df_plot=df_plot.orderBy(x_column, ascending=False).head(12)
        x=[]
        y=[]
        for i in df_plot:
            x.append(str(i.asDict()[x_column]))
            y.append(i.asDict()["sum("+y_column+")"])
        print(x,y)      
        # fig, axs = plt.subplots(1)
        # fig.subplots_adjust(hspace=0.75)
        # axs[0].plot(x,y)
        # axs[0].set_xticklabels(x, rotation=90)
        # axs[0].set_title("Distribution of "+ y_column+" over " +x_column)
        # axs[0].set_xlabel(x_column)
        # axs[0].set_ylabel(y_column)

        plt.plot(x,y)
        plt.xticks(rotation=90)
        plt.title("Distribution of "+ y_column+" over " +x_column)
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        # plt.show()
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png')
        bytes_image.seek(0)
        return bytes_image


    def countVisualiztion(self,sales_df,x_column):
        df_plot=sales_df.groupBy(x_column).count()
        # window = Window.partitionBy(df_plot[x_column]).orderBy(df_plot["sum("+y_column+")"].desc())
        # df_plot=df_plot.select('*', rank().over(window).alias('rank')).filter(df_plot['rank'] <= 2) 
        print(df_plot)
        df_plot=df_plot.orderBy("count", ascending=False).head(10)
        x=[]
        y=[]
        for i in df_plot:
            x.append(str(i.asDict()[x_column]))
            y.append(i.asDict()["count"])
       
        plt.bar(x,y)
        plt.title("Count Distribution of  " +x_column)
        plt.xlabel(x_column)
        plt.ylabel("count")
        # plt.show()
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png')
        bytes_image.seek(0)
        return bytes_image

           

    def minVisualiztion(self,sales_df):
            x_column=input("enter x column")
            y_column=input("enter y column")
            df_plot=sales_df.groupBy(x_column).min(y_column)
            print(df_plot) 
            x=df_plot.toPandas()[x_column].values.tolist()
            y=df_plot.toPandas()["min("+y_column+")"].values.tolist()
            plt.bar(x,y)
            plt.title("Distribution of "+ y_column+" over " +x_column)
            plt.xlabel(x_column)
            plt.ylabel(y_column)
            plt.show()

    def maxVisualiztion(self,sales_df):
            x_column=input("enter x column")
            y_column=input("enter y column")
            df_plot=sales_df.groupBy(x_column).max(y_column)
            print(df_plot)
            x=df_plot.toPandas()[x_column].values.tolist()
            y=df_plot.toPandas()["max("+y_column+")"].values.tolist()
            plt.bar(x,y)
            plt.title("Distribution of "+ y_column+" over " +x_column)
            plt.xlabel(x_column)
            plt.ylabel(y_column)
            plt.show()

    def corrHeatMap(self,df):
        
        corr = df.toPandas().corr()
        
        f, ax = plt.subplots(figsize=(11, 9))
        cmap = sns.diverging_palette(220, 10, as_cmap=True)

        sns.heatmap(corr,  cmap=cmap, vmax=.3, center=0,
                    square=True, linewidths=.5, cbar_kws={"shrink": .5})
       
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png')
        bytes_image.seek(0)
        return bytes_image


    def sumBoxplotVisualiztion(self,sales_df,x_column,y_column):
        # x_column=input("enter x column")
        # y_column=input("enter y column")
        # cmap = sns.diverging_palette(220, 10, as_cmap=True)
        f, ax = plt.subplots(figsize=(11, 9))
        df_plot=sales_df.head(10)
        
        x=[]
        y=[]
        for i in df_plot:
            x.append(str(i.asDict()[x_column]))
            y.append(i.asDict()[y_column])
        print(x,y)
        sns.boxplot(x,y,data=sales_df.toPandas())
        # plt.title("Distribution of "+ y_column+" over " +x_column)
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png')
        bytes_image.seek(0)
        return bytes_image
        

    def multicolinearityVisualization(self,sales_df):
        print("Multicol Visualization")
        vif = pd.DataFrame() 
        # X =sales_df.toPandas().iloc[:,:-1]
        # vif_data["feature"] = sales_df.toPandas().columns
        # X = sales_df[['date_block_num','shop_id','item_id','item_price','item_cnt_day']]
        # sales_df=sales_df._get_numeric_data()
        # for i in range(len(sales_df.toPandas().columns)):
        #     vif.append(variance_inflation_factor(sales_df.toPandas().values, i))
            # variance_inflation_factor
        # result=self.spark.createDataFrame( schema=['columns','IQR', 'quantile_25','quantile_50','quantile_75'])
        print(sales_df.printSchema())
        columns=[]
        for c,d in zip(sales_df.columns, sales_df.dtypes) :
            if d[1] in ("int","double"):
                columns.append(c)
        print(columns)
        X=sales_df[[columns]].toPandas().iloc[:,:-1]
        # print
        vif["variables"] = X.columns
        print(X.shape[1])
        vif["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
        # vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(len(X.columns))] 
  
        print(vif)
        x=vif["variables"].values.tolist()
        y=vif["VIF"].values.tolist()
        plt.plot(x,y,color='blue')
        plt.xlabel('variables')
        plt.ylabel('VIF')
        plt.title('Multicolinearity Visualization')
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png')
        bytes_image.seek(0)
        return bytes_image

    def outlierVisualization(self,sales_df,x_column,y_column):
        # plt.title('Showing Outliers')
        f, ax = plt.subplots(figsize=(11, 9))
        df_plot=sales_df
        df=sales_df.toPandas()
    
        x=df[x_column].values.tolist()
        y=df[y_column].values.tolist()

        # y=list(map(str,y))  
        print("y column is converted into string")          
        # print(x,y)
        # sns.boxplot(x,y,data=sales_df.toPandas())
        sns.boxplot(data=sales_df.toPandas(), y = x, x = y)
        print("box plot created ")
        plt.title('With Outlier Handling')
        handle_outliers(data=df, y=x,x=y,plotter=sns.boxplot)
        print("Boxplot with outlier has been created ")
        bytes_image = io.BytesIO()
        plt.savefig(bytes_image, format='png')
        bytes_image.seek(0)
        return bytes_image
    
        
    
    def numbers_to_strings(self,argument,sales_df): 
        switcher = { 
            # 0: print(sales_df.select('*').describe().show()), 
            # 1: print(sales_df.printSchema()), 
            # 2: print(self.getQuantileRange(sales_df)), 
            # 3: print(self.getMissingValueCount(sales_df)),
            # 4: print(self.getCorrelationMatrix(sales_df)),
            # 5: print(self.covertDateintoMYD(sales_df)),
            # 6: print(self.deduplicationofRows(sales_df)),
            # 7: print(self.interquantileRange(sales_df)),
            # 8: print(self.detectOutilers(sales_df)),
            # 9: print(self.sumVisualiztion(sales_df)),
            # 10: print(self.sumBoxplotVisualiztion(sales_df)),
            # 11: print(self.corrHeatMap(sales_df))
            # 12: print(self.multicolinearityVisualization(sales_df))
        } 
        return switcher.get(argument, "nothing")
  
    # get() method of dictionary data type returns  
    # value of passed argument if it is present  
    # in dictionary otherwise second argument will 
    # be assigned as default value of passed argument 

    def __init__(self, spark, sc):
        self.spark = spark
        self.sc=sc
        # sales_file_path = os.path.join(dataset_path, 'sales_train.csv')
        # sales_df = self.spark.read.option("inferSchema","true").option("header","true").csv(sales_file_path)
        # sql_df=sc.read.option("inferSchema","true").option("header","true").csv(sales_file_path)
        # sales_df=sc.createDataFrame(sales_raw_RDD)
        # argument=int(input("Please enter argument"))
        # s=Switcher()


        # self.numbers_to_strings(argument,sales_df)

        # print(no_of_rows)

        # ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        # self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_raw_data_header)\
        #     .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
        # Load movies data for later use
         


