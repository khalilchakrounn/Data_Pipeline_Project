from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark
import random
from pyspark.sql.functions import countDistinct, count, when
from pyspark.sql.functions import col, collect_list, year, month, dayofmonth
import matplotlib.pyplot as plt
import numpy as np

def main():
    sc = pyspark.SparkContext('local')
    sc = SparkSession(sc) 
    sc.setLogLevel('ERROR')

    #importing the data, the file is renamed by the nifi process to mergedData and it contains all data downloaded
    df= spark.read.csv("./../DataPipeline/data merged/mergedData", header=True, inferSchema=True, sep=';')
    #maybe change the spark to sc if it doesn't work

    # first task of cleaning is dropping the duplicates
    df= df.dropDuplicates()

    df.columns
    #['Civilité', 'Nom', 'Prénom', 'Mandat', 'Circonscription', 'Département', 'Candidat', 'Date de publication']

    #changing the values in Civilité from .M to H  and from Mee. to F 
    df = df.withColumn('Civilité', when(df['Civilité'] == 'M.', 'H').otherwise('F'))

    #couting unique values per column
    for column in df.columns:
        count_unique = df.select(column).agg(countDistinct(column)).collect()[0][0]
        print(f"number of unique values in  column '{column}': {count_unique}")

#number of unique values in  column 'Civilité': 3                         
#number of unique values in  column 'Nom': 10310                          
#number of unique values in  column 'Prénom': 1616                        
#number of unique values in  column 'Mandat': 30
#number of unique values in  column 'Circonscription': 8891               
#number of unique values in  column 'Département': 111
#number of unique values in  column 'Candidat': 66
#number of unique values in  column 'Date de publication': 12


    #next step, check if there are typos in these columns to fix them
    for column in ['Civilité','Date de publication','Candidat']:
        valeurs_distinctes = df.select(column).distinct().agg(collect_list(col(column))).collect()[0][0]
        print(f" unique values in column '{column}': {valeurs_distinctes}")

#unique values in column 'Civilité': ['M.', 'Civilité', 'Mme']
#unique values in column 'Date de publication': ['08/02/2022', '01/02/2022', '03/02/2022', '10/02/2022', '24/02/2022', '22/02/2022', '07/03/2022', '01/03/2022', '17/02/2022', '03/03/2022', 'Date de publication', '15/02/2022']

## no typos detected. we can perserve the data at this state.
# we notice that all data was geenrated at either 03/2022 or 02/2022 which means no need to keep the year in the date

    df = df.withColumn("Month", month(col("Date de publication"))).withColumn("day", dayofmonth(col("Date de publication")))
    df=df.drop('Date de publication')

    #saving the processed data in a file as processedData
    df.write.csv("./../DataPipeline/data merged/processedData", header=True, mode='overwrite')

#this part is all about creating some aggregations and then to use them for plots
# we will start by groupping the the votes by candiat
    vote_per_candidat= df.groupBy("Candidat").agg(count("*").alias("Nombre_de_votes")).orderBy("Nombre_de_votes", ascending=False)

    # and then by groupping the the votes by candiat by sex
    vote_per_candidat_per_genre= df.groupBy("Candidat","Civilité").agg(count("*").alias("Nombre_de_votes")).orderBy("Candidat", ascending=False)
    vote_per_candidat_per_genre=vote_per_candidat_per_genre.filter("Nombre_de_votes >= 200")
    vote_per_candidat = vote_per_candidat.toPandas()
    vote_per_candidat_per_genre = vote_per_candidat_per_genre.toPandas()
    fig, ax = plt.subplots(figsize=(10, 6))
    hommes = vote_per_candidat_per_genre[vote_per_candidat_per_genre ['Civilité'] == 'H']
    femmes = vote_per_candidat_per_genre[vote_per_candidat_per_genre ['Civilité'] == 'F']
    ax.barh(hommes['Candidat'], hommes['Nombre_de_votes'], label='Hommes', color='blue')
    ax.barh(femmes['Candidat'], femmes['Nombre_de_votes'], label='Femmes', color='pink')
    ax.set_xlabel('Candidat')
    ax.set_ylabel('Nombre de votes')
    ax.set_title('Nombre de votes par candidat et par genre')
    ax.legend()
    plt.show()



    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie(vote_per_candidat['Nombre_de_votes'], labels=vote_per_candidat['Candidat'], counterclock=False)
    ax.set_title('Répartition des votes par candidat')
    plt.show()


main()
