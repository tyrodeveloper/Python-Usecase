import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib.pyplot as plt
import seaborn as sns
# from wordcloud import WordCloud,STOPWORDS
# import squarify

import os
print(os.listdir("C:\\Users\\lenovo\\Downloads\\Input"))

df_survey_results = pd.read_csv('C:\\Users\\lenovo\\Downloads\\Input\\survey_results_public.csv')

plt.figure(figsize=(10,8))

df_survey_results['WakeTime'].value_counts().sort_values(ascending=True).plot.barh(width=0.9,color=sns.color_palette('inferno_r',15))
plt.savefig('sta.png')
plt.show()


f,ax=plt.subplots(1,2,figsize=(15,8))

sns.countplot(x="WakeTime", hue="JobSatisfaction", data=df_survey_results, ax=ax[0])
sns.countplot(x='WakeTime', hue='CareerSatisfaction', data=df_survey_results, ax=ax[1])
ax[0].set_title('Effect of wake up time in job satisfaction', fontsize=18)
ax[1].set_title('Effect of wake up time in career satisfaction', fontsize=18)
ax[0].tick_params(axis='x', labelsize=8,rotation = 90)
ax[1].tick_params(axis='x', labelsize=8, rotation=90)
plt.setp(ax[0].get_legend().get_texts(), fontsize='8') # for legend text
plt.setp(ax[0].get_legend().get_title(), fontsize='10') # for legend title
plt.setp(ax[1].get_legend().get_texts(), fontsize='8') # for legend text
plt.setp(ax[1].get_legend().get_title(), fontsize='10') # for legend title
plt.show()


f, ax = plt.subplots(1,2,figsize = (15,8))

df_survey_results.Exercise.value_counts().sort_values(ascending=True).plot.barh(width=0.9,color=sns.color_palette('inferno_r',15), ax = ax[0])
df_survey_results.SkipMeals.value_counts().sort_values(ascending=True).plot.barh(width=0.9,color=sns.color_palette('inferno_r',15), ax = ax[1])
ax[0].tick_params(axis='y', labelsize=8)
ax[1].tick_params(axis='y', labelsize=8)
ax[0].set_title('How developers exercise in a week', fontsize=8)
ax[1].set_title('How developers skip their meals in a week', fontsize=8)
plt.show()

f, ax = plt.subplots(1,2,figsize = (15,8))

df_survey_results.Exercise.value_counts().sort_values(ascending=True).plot.barh(width=0.9,color=sns.color_palette('inferno_r',15), ax = ax[0])
df_survey_results.SkipMeals.value_counts().sort_values(ascending=True).plot.barh(width=0.9,color=sns.color_palette('inferno_r',15), ax = ax[1])
ax[0].tick_params(axis='y', labelsize=8)
ax[1].tick_params(axis='y', labelsize=8)
ax[0].set_title('How developers exercise in a week', fontsize=8)
ax[1].set_title('How developers skip their meals in a week', fontsize=8)
plt.show()


f , ax = plt.subplots(1,2, figsize = (15, 8))


df_survey_results.HoursComputer.value_counts().sort_values(ascending= True).plot.barh(width =0.9,color = sns.color_palette('inferno_r', 15), ax = ax[0])
df_survey_results.HoursOutside.value_counts().sort_values(ascending= True).plot.barh(width =0.9,color = sns.color_palette('inferno_r', 15), ax = ax[1])
ax[0].tick_params(axis='y', labelsize=8)
ax[1].tick_params(axis='y', labelsize=8)
ax[0].set_title('Time spent on computer in a day ', fontsize=8)
ax[1].set_title('Time spent on outside in a day', fontsize=8)
plt.show()

plt.figure(figsize=(15,8))
df_survey_results.ErgonomicDevices.value_counts().sort_values(ascending= True).plot.barh(width =0.9,color = sns.color_palette('inferno_r', 15), ax = ax[0])
plt.title('Ergonomic devices use by developers', fontsize=8)
plt.yticks(fontsize = 8)
plt.show()


f, ax = plt.subplots(2,4,figsize = (15,15))
