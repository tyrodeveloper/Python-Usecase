import os
from functions import EdaToolFunctions
from flask import Flask,render_template,send_file,Response
from flask import request
from pyspark.sql import SparkSession
from flask import Blueprint
import os
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from flask import Flask, flash, request, redirect, render_template,url_for
from werkzeug.utils import secure_filename
main = Blueprint('main', __name__) 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from io import BytesIO
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
plt.style.use('ggplot')




def create_app(spark_session,sqlContext):
    global Eda,Eda1,Eda2,Eda3,Eda4,Eda5,Eda6
    global Eda7 
    Eda2 = None
    print(Eda2)
    Eda = EdaToolFunctions(spark_session,sqlContext) 
    Eda1 = EdaToolFunctions(spark_session,sqlContext)
    Eda2 = EdaToolFunctions(spark_session,sqlContext)
    Eda3 = EdaToolFunctions(spark_session,sqlContext)
    Eda4 = EdaToolFunctions(spark_session,sqlContext)
    Eda5 = EdaToolFunctions(spark_session,sqlContext)
    Eda6 = EdaToolFunctions(spark_session,sqlContext)
    Eda7 = EdaToolFunctions(spark_session,sqlContext)

    global app
    # cache = Cache(config={'CACHE_TYPE': 'null'})

    app = Flask(__name__)
    app.register_blueprint(main)
    app.secret_key = "secret key"
    app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    app.config["CACHE_TYPE"] = 'null'
    # with app.app_context():
    #     cache.clear()
    path = os.getcwd()
    # file Upload
    UPLOAD_FOLDER = os.path.join(path, 'uploads')

    if not os.path.isdir(UPLOAD_FOLDER):
        os.mkdir(UPLOAD_FOLDER)

    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


  
    return app



def allowed_file(filename):
    ALLOWED_EXTENSIONS = set(['txt', 'csv'])

    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@main.route('/')
def upload_form():
    # app.app_context().Cache = 'null'
    return render_template('upload.html', name = 'Welcome to Pyspark powered EDA Tool')



# @main.route("/")
# def index():
#     return render_template('home.html')

@main.route('/stats', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash('No file selected for uploading')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            flash('File successfully uploaded')
            global sales_df
            sales_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            sales_df = Eda.spark.read.option("inferSchema","true").option("header","true").csv(sales_file_path)
            print("file read completed")
            return render_template('home.html', name = 'Analytical Stats')
        else:
            flash('Allowed file types are txt, csv')
            return redirect(request.url)

@main.route('/redirect')
def redirectURL():
    return render_template('home.html', name = 'Analytical Stats')

@main.route('/getBasicStats', methods=['GET', 'POST'])
def getBasicStats():
        global sales_df
        df1=sales_df.select('*').describe().toPandas()
        df2=sales_df.printSchema()
        
        rows = sales_df.count()
        columns = len(sales_df.columns)
        size = (rows, columns)
        # print("skewness of data ",sales_df.skew())
        sales_df=sales_df.drop_duplicates()
        rows = sales_df.count()
        sales_df.count()
        columns = len(sales_df.columns)
        size1 = (rows, columns)
        return render_template('basicstats.html', name = 'Basic Stats',tables=[df1.to_html(classes='data')], titles=df1.columns.values, size=size,size1=size1)

        # return render_template('basicstats.html', name = 'Basic Stats', df1=df1, df2=df2)

@main.route('/getQuantileandInterQuantilrRange', methods=['GET', 'POST'])
def getQandIQRange():
        global sales_df
        
        bounds,result=Eda.interquantileRange(sales_df)
        # print(Eda.detectOutilers(sales_df))
        result=result.toPandas()
        
        return render_template('quantileRange.html', name = 'Quantile and InterQuantile Range',tables=[result.to_html(classes='data')], titles=result.columns.values)

@main.route('/detectOutlier', methods=['GET', 'POST'])
def getOutlier():
        global sales_df
        
        result=Eda.detectOutilers(sales_df)
        result=result.toPandas()
        
        return render_template('quantileRange.html', name = 'Detect Outlier',tables=[result.to_html(classes='data')], titles=result.columns.values)



@main.route('/exploratoryVisualization')
def exploratoryVisualization():
    return render_template('visualizationList.html', name = 'Exploratory Viualization')

@main.route('/columnsFurtherTreatment')
def featureEngg():
    global sales_df
    columns=sales_df.columns 
    
    return render_template('featureEnggColumn.html', name = 'Column Further treatment ',columns=columns)

@main.route('/conversion' ,methods=['GET', 'POST'])
def conversionOfDate():
    column = request.form['x_column']
    global sales_df
    sales_df=Eda.covertDateintoMYD(sales_df,column)
    print('exit')
    columns=sales_df.toPandas().columns
    return render_template('corrHeatmap.html', name = columns)



@main.route('/correlationHeatMap')
def corrHeatMapVisualization():
        # corr = sales_df.toPandas().corr()

    bytes_obj = Eda1.corrHeatMap(sales_df)
    # del Eda1
    
    response=send_file(bytes_obj,attachment_filename='plot.png',mimetype='image/png')
    
    return response

@main.route('/misingValueHeatMap')   
def missingValueVisualization():
    bytes_obj = Eda.getMissingValueCount(sales_df)
    # del Eda
    response=send_file(bytes_obj,attachment_filename='plot1.png',mimetype='image/png')
    
    return response

@main.route('/multicolinearityVisualization', methods=['GET', 'POST'])   
def multicolVisualization():
    print("BEFORE CALLING")
    bytes_obj = Eda2.multicolinearityVisualization(sales_df)
    # del Eda2
    response=send_file(bytes_obj,attachment_filename='plot2.png',mimetype='image/png')
    # bytes_obj.seek(0, os.SEEK_END)
    # size = bytes_obj.tell()
    # bytes_obj.seek(0)
    # response.headers.extend({
    #     'Content-Length': size,
    #     'Cache-Control': 'no-cache'
    # })
    return response
    
   

@main.route('/DataDistributionsumPlotBar')   
def DataDist():
    global sales_df
    columns=sales_df.columns 
    # x_column=[]
    # y_column=[]
    # for c,d in zip(columns,sales_df.dtypes) :
            # if d[1] in ("int","double"):
            #     y_column.append(c)    
            # else:
            #     x_column.append(c)
    return render_template('sumdataDist.html', name = 'Data Distribution Viualization', columns=columns)


@main.route('/sumdataDistribution' ,methods=['GET', 'POST'])
def sumDataDistribution():
    x = request.form['x_column']
    y=request.form['y_column']
    global sales_df
    bytes_obj= Eda3.sumVisualiztion(sales_df,x,y)
    # del Eda3

    
    response= send_file(bytes_obj,attachment_filename='plot3.png',mimetype='image/png') 
    # bytes_obj.seek(0, os.SEEK_END)
    # size = bytes_obj.tell()
    # bytes_obj.seek(0)
    # response.headers.extend({
    #     'Content-Length': size,
    #     'Cache-Control': 'no-cache'
    # })
    return response


@main.route('/TimeSeriesVisualization')   
def TSVisual():
    global sales_df
    columns=sales_df.columns 
    return render_template('linedataDist.html', name = 'Data Distribution Viualization',columns=columns)


@main.route('/linedataDistribution' ,methods=['GET', 'POST'])
def TSPLot():
    x = request.form['x_column']
    y=request.form['y_column']
    global sales_df
    bytes_obj= Eda4.lineSumVisualiztion(sales_df,x,y)
    # del Eda4
    
    response=send_file(bytes_obj,attachment_filename='plot4.png',mimetype='image/png')
    # bytes_obj.seek(0, os.SEEK_END)
    # size = bytes_obj.tell()
    # bytes_obj.seek(0)
    # response.headers.extend({
    #     'Content-Length': size,
    #     'Cache-Control': 'no-cache'
    # })
    return response


@main.route('/DataDistributioncountPlotBar')   
def DataDistcount():
    global sales_df
    columns=sales_df.columns 
    
    return render_template('countdataDist.html', name = 'Data Distribution Viualization',columns=columns)


@main.route('/countdataDistribution' ,methods=['GET', 'POST'])
def sumDataDistributioncount():
    x = request.form['x_column']
    global sales_df
    bytes_obj= Eda5.countVisualiztion(sales_df,x)
    # del Eda5
   
    response=send_file(bytes_obj,attachment_filename='plot5.png',mimetype='image/png')
    # bytes_obj.seek(0, os.SEEK_END)
    # size = bytes_obj.tell()
    # bytes_obj.seek(0)
    # response.headers.extend({
    #     'Content-Length': size,
    #     'Cache-Control': 'no-cache'
    # })
    return response

@main.route('/DataDistributionPlotBoxplot')   
def BoxDataDist():
    global sales_df
    columns=sales_df.columns 
    
    return render_template('boxdataDist.html', name = 'Data Distribution Viualization',columns=columns)


@main.route('/boxdataDistribution' ,methods=['GET', 'POST'])
def boxDataDistributioncount():
    x = request.form['x_column']
    y = request.form['y_column']
    global sales_df
    bytes_obj= Eda6.sumBoxplotVisualiztion(sales_df,x,y)
    # del Eda6
    
    response=send_file(bytes_obj,attachment_filename='plot6.png',mimetype='image/png')
    # bytes_obj.seek(0, os.SEEK_END)
    # size = bytes_obj.tell()
    # bytes_obj.seek(0)
    # response.headers.extend({
    #     'Content-Length': size,
    #     'Cache-Control': 'no-cache'
    # })
    return response

@main.route('/outlierVisualization')   
def outlierDist():
    global sales_df
    columns=sales_df.columns 
    
    return render_template('outlierdataDist.html', name = 'Outlier Viualization',columns=columns)


@main.route('/outlierPlot' ,methods=['GET', 'POST'])
def outlierPlot():
    x = request.form['x_column']
    y = request.form['y_column']
    global sales_df
    bytes_obj= Eda7.outlierVisualization(sales_df,x,y)
    # del Eda7
    response=send_file(bytes_obj,attachment_filename='plot7.png',mimetype='image/png')
    # bytes_obj.seek(0, os.SEEK_END)
    # size = bytes_obj.tell()
    # bytes_obj.seek(0)
    # response.headers.extend({
    #     'Content-Length': size,
    #     'Cache-Control': 'no-cache'
    # })
    return response

@main.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r