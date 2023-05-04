import pandas as pd
import matplotlib.pyplot as plt

prog_book_df = pd.read_csv('prog_book.csv')
rating_df = prog_book_df['Rating']
reviews_df = prog_book_df['Reviews'].apply(lambda x: x.replace(",", "")).astype(int)
num_pages_df = prog_book_df['Number_Of_Pages']
price_df = prog_book_df['Price']

q1 = rating_df.quantile(.25)
q3 = rating_df.quantile(.75)
IQR = q3 - q1
high = q3 + 1.5 * IQR
low = q1 - 1.5 * IQR
num_outliers = 0
for i in rating_df.index:
    if(rating_df[i] > high):
        print("Ratings: Outlier detected (high), value = " + str(rating_df[i]) + ", index = " + str(i))
        num_outliers += 1
    if(rating_df[i] < low):
        print("Ratings: Outlier detected (low), value = " + str(rating_df[i]) + ", index = " + str(i))
        num_outliers += 1
print("Number of outliers (Ratings): " + str(num_outliers))

q1 = reviews_df.quantile(.25)
q3 = reviews_df.quantile(.75)
IQR = q3 - q1
high = q3 + 1.5 * IQR
low = q1 - 1.5 * IQR
num_outliers = 0
for i in reviews_df.index:
    if(reviews_df[i] > high):
        print("Reviews: Outlier detected (high), value = " + str(reviews_df[i]) + ", index = " + str(i))
        num_outliers += 1
    if(reviews_df[i] < low):
        print("Reviews: Outlier detected (low), value = " + str(reviews_df[i]) + ", index = " + str(i))
        num_outliers += 1
print("Number of outliers (Reviews): " + str(num_outliers))

q1 = num_pages_df.quantile(.25)
q3 = num_pages_df.quantile(.75)
IQR = q3 - q1
high = q3 + 1.5 * IQR
low = q1 - 1.5 * IQR
num_outliers = 0
for i in num_pages_df.index:
    if(num_pages_df[i] > high):
        print("Number of Pages: Outlier detected (high), value = " + str(num_pages_df[i]) + ", index = " + str(i))
        num_outliers += 1
    if(num_pages_df[i] < low):
        print("Number of Pages: Outlier detected (low), value = " + str(num_pages_df[i]) + ", index = " + str(i))
        num_outliers += 1
print("Number of outliers (Number of Pages): " + str(num_outliers))

q1 = price_df.quantile(.25)
q3 = price_df.quantile(.75)
IQR = q3 - q1
high = q3 + 1.5 * IQR
low = q1 - 1.5 * IQR
num_outliers = 0
for i in price_df.index:
    if(price_df[i] > high):
        print("Price: Outlier detected (high), value = " + str(price_df[i]) + ", index = " + str(i))
        num_outliers += 1
    if(price_df[i] < low):
        print("Price: Outlier detected (low), value = " + str(price_df[i]) + ", index = " + str(i))
        num_outliers += 1
print("Number of outliers (Price): " + str(num_outliers))

rating_df.plot(kind='box')

reviews_df.plot(kind='box')

num_pages_df.plot(kind='box')

price_df.plot(kind='box')

#find all possible pairs of features from book_df
#and perform DBSCAN clustering on each pair
#plot the results
#Path: p4.ipynb
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler, LabelEncoder

prog_book_df = pd.read_csv('prog_book.csv')
prog_book_df['Reviews'] = prog_book_df['Reviews'].apply(lambda x: x.replace(",", "")).astype(int)
encoder = LabelEncoder()
prog_book_df['Type'] = encoder.fit_transform(prog_book_df['Type'])
prog_book_df['Book_title'] = encoder.fit_transform(prog_book_df['Book_title'])
prog_book_df['Description'] = encoder.fit_transform(prog_book_df['Description'])
features = ['Rating', 'Reviews', 'Book_title', 'Description', 'Type', 'Number_Of_Pages', 'Price']
for i in range(len(features)):
    for j in range(i + 1, len(features)):
        X = prog_book_df[[features[i], features[j]]]
        X = StandardScaler().fit_transform(X)
        db = DBSCAN(eps=0.8, min_samples=10).fit(X)
        labels = db.labels_
        outliers = np.where(labels == -1)
        plt.scatter(X[:, 0], X[:, 1], label='Not Outliers')
        plt.scatter(X[outliers, 0], X[outliers, 1], label='outliers')
        plt.xlabel(features[i])
        plt.ylabel(features[j])
        plt.legend()
        print(prog_book_df[labels == -1])
        print("number of outliers:" + str(len(outliers[0])))
        '''core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
        core_samples_mask[db.core_sample_indices_] = True
        unique_labels = set(labels)
        colors = [plt.cm.Spectral(each)
                  for each in np.linspace(0, 1, len(unique_labels))]
        for k, col in zip(unique_labels, colors):
            if k == -1:
                # Black used for noise.
                col = [0, 0, 0, 1]
            class_member_mask = (labels == k)
            xy = X[class_member_mask & core_samples_mask]
            plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col),
                     markeredgecolor='k', markersize=14)
            xy = X[class_member_mask & ~core_samples_mask]
            plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col),
                     markeredgecolor='k', markersize=6)'''
        plt.title('DBSCAN clustering: ' + features[i] + ' vs ' + features[j])
        plt.show()

prog_book_df = pd.read_csv('prog_book.csv')
prog_book_df['Reviews'] = prog_book_df['Reviews'].apply(lambda x: x.replace(",", "")).astype(int)
encoder = LabelEncoder()
prog_book_df['Type'] = encoder.fit_transform(prog_book_df['Type'])
prog_book_df['Book_title'] = encoder.fit_transform(prog_book_df['Book_title'])
prog_book_df['Description'] = encoder.fit_transform(prog_book_df['Description'])
features = ['Rating', 'Reviews', 'Book_title', 'Description', 'Type', 'Number_Of_Pages', 'Price']
for i in range(len(features)):
    for j in range(i + 1, len(features)):
        for z in range(i + j + 1, len(features)):
            X = prog_book_df[[features[i], features[j], features[z]]]
            X = StandardScaler().fit_transform(X)
            db = DBSCAN(eps=0.8, min_samples=10).fit(X)
            labels = db.labels_
            outliers = np.where(labels == -1)
            fig = plt.figure()
            ax = fig.add_subplot(projection='3d')
            ax.scatter(X[:, 0], X[:, 1], X[:, 2], label='Not Outliers')
            ax.scatter(X[outliers, 0], X[outliers, 1], X[outliers, 2], label='outliers')
            ax.set_xlabel(features[i])
            ax.set_ylabel(features[j])
            ax.set_zlabel(features[z])
            ax.set_title(str('DBSCAN clustering: ' + features[i] + ' vs ' + features[j] + ' vs ' + features[z]))
            ax.legend()
            print(prog_book_df[labels == -1])
            print("number of outliers:" + str(len(outliers[0])))
            plt.show()
