import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
# %matplotlib inline
import nltk
from nltk.classify import SklearnClassifier
from sklearn.model_selection import train_test_split
from wordcloud import WordCloud,STOPWORDS
from functions import get_cleaned_tweets,get_word_features,extract_features,wordcloud_draw
# classifier=[]
def prediction() :
    df = pd.read_csv("train.csv")
    # df1 = pd.read_csv("test.csv")
    
    df['text'] = df['text'].str.replace("[^A-Za-z ]", "")
    train , test = train_test_split(df,test_size=0.3)
    train_1 = train[ train['target'] == 1]
    train_1 = train_1['text']
    train_0 = train[ train['target'] == 0]
    train_0 = train_0['text']

    
    tweets = get_cleaned_tweets(train)
    w_features = get_word_features(tweets)
    # plt = wordcloud_draw(w_features)
    test_0 = test[test['target'] == 0]
    test_0 = test_0['text']
    test_1 = test[test['target'] == 1]
    test_1 = test_1['text']
    training_set = nltk.classify.apply_features(extract_features,tweets)
    classifier = nltk.NaiveBayesClassifier.train(training_set)
    fake_cnt = 0
    real_cnt = 0
    for obj in test_0: 
        res =  classifier.classify(extract_features(obj.split()))
        if(res == 0): 
            fake_cnt = fake_cnt + 1
    for obj in test_1: 
        res =  classifier.classify(extract_features(obj.split()))
        if(res == 1): 
            real_cnt = real_cnt + 1
    print('[Fake Tweets]: %s/%s '  % (len(test_0),fake_cnt))        
    print('[Real Tweets]: %s/%s '  % (len(test_1),real_cnt))    
    return (fake_cnt+real_cnt)/(len(test_0)+len(test_1))


def prediction_sentiment(sentiment):
    df = pd.read_csv("train.csv")
    # df1 = pd.read_csv("test.csv")
    
    df['text'] = df['text'].str.replace("[^A-Za-z ]", "")
    train , test = train_test_split(df,test_size=0.3)
    train_1 = train[ train['target'] == 1]
    train_1 = train_1['text']
    train_0 = train[ train['target'] == 0]
    train_0 = train_0['text']

    
    tweets = get_cleaned_tweets(train)
    w_features = get_word_features(tweets)
    # plt = wordcloud_draw(w_features)
    test_0 = test[test['target'] == 0]
    test_0 = test_0['text']
    test_1 = test[test['target'] == 1]
    test_1 = test_1['text']
    training_set = nltk.classify.apply_features(extract_features,tweets)
    classifier = nltk.NaiveBayesClassifier.train(training_set)
    res =  classifier.classify(extract_features(sentiment.split()))
    Sentiment_type="Neutral"
    if (res==0):
        Sentiment_type="Don't Worry it's not Dangerous "
    if(res==1):
        Sentiment_type="Dangerous"
    return Sentiment_type


def prediction_cluster():
    document=[]
    df = pd.read_csv("train.csv")

    df['text'] = df['text'].str.replace("[^A-Za-z ]", "")
    document = df['text'].values

    vectorizer = TfidfVectorizer(stop_words='english')
    X = vectorizer.fit_transform(document)    
    true_k = 2
    model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
    model.fit(X)
    order_centroids = model.cluster_centers_.argsort()[:, ::-1]
    terms = vectorizer.get_feature_names()
    for i in range(true_k):
        print("Cluster %d:" % i),
    for ind in order_centroids[i, :10]:
        print(' %s' % terms[ind])

    print("\n")
    print("Prediction")
    document_test
    df = pd.read_csv("train.csv")

    df['text'] = df['text'].str.replace("[^A-Za-z ]", "")
    document = df['text'].values
    X = vectorizer.transform(["Nothing is easy in cricket. Maybe when you watch it on TV, it looks easy. But it is not. You have to use your brain and time the ball."])
    predicted = model.predict(X)
    print(predicted)

