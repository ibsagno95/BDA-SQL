
""" Système de gestion d'emprunt dans une librairie"""

## Importation des librairies
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf 
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

## instanciation du client spark

spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName("emprunt_librairie")\
                    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


## Création des tables 
## Table des auteurs
L1 = [("07890", "Jean Paul Sartre"), ("05678", "Pierre de Ronsard")] ## On crée une liste contenant les informations
RDD1 = spark.sparkContext.parallelize(L1)   ## transformation en RDD
Author = RDD1.toDF(["aid", "name"])          ## on nomme les colonnes
Author.show()
Author.createOrReplaceTempView("Author_SQL")       ## Création d'une table SQL

## Table des livres
L2 = [("0001", "L'existentialisme est un humanisme","Philosophie"), ("0002", "Huis clos. Suivi de Les Mouches","Philosophie"),
      ("0003", "Mignonne allons voir si la rose","Poème"),("0004", "Les Amours","Poème")]
RDD2 = spark.sparkContext.parallelize(L2)
Book = RDD2.toDF(["bid", "title","category"])
Book.show()
Book.createOrReplaceTempView("Book_SQL")

## Table des étudiants
L3 = [("S15", "toto","Maths"), ("S16", "popo","Eco"),
      ("S17", "fofo","Mécanique")]
RDD3 = spark.sparkContext.parallelize(L3)
Student= RDD3.toDF(["sid", "sname","dept"])
Student.show()
Student.createOrReplaceTempView("Student_SQL")

## Table des livres par auteurs
L4 = [("07890", "0001"), ("07890","0002"),
      ("05678", "0003"),("05678","0004")]
RDD4 = spark.sparkContext.parallelize(L4)
Write= RDD4.toDF(["aid", "bid"])
Write.show()
Write.createOrReplaceTempView("Write_SQL")

## Table des emprunts
L5 = [("S15", "0003","02-01-2020","01-02-2020"), ("S15", "0002","13-06-2020","null"),
      ("S15", "0001","13-06-2020","13-10-2020"),("S16","0002","24-01-2020","24-01-2020"),("S17","0001","12-04-2020","01-07-2020")]
RDD5 = spark.sparkContext.parallelize(L5)
Borrow= RDD5.toDF(["sid", "bid","checkout_time","return_time"])
Borrow.show()
Borrow.createOrReplaceTempView("Borrow_SQL")

## Les titres de tous les livres que l'étudiant sid='S15' a emprunté
## SQL
print(" ********* Titres des livres empruntés par l'étudiant S15")
spark.sql(""" SELECT title 
              FROM Book_SQL 
              INNER JOIN Borrow_SQL ON  Borrow_SQL.bid = Book_SQL.bid  
              WHERE Borrow_SQL.sid = "S15" """)\
      .show()
## DSL
Book.join(Borrow,["bid"])\
    .select(col("title"))\
    .filter(col("sid")=="S15")\
    .show()

## Les titres de tous les livres qui n'ont jamais été empruntés par un étudiant.
## SQL
print("***** Titres des livres jamais empruntés *******")
spark.sql(""" SELECT title
            FROM Book_SQL 
            LEFT JOIN Borrow_SQL on   Borrow_SQL.bid = Book_SQL.bid  
            WHERE Borrow_SQL.sid  IS NULL """)\
    .show()

## DSL
Book.join(Borrow,["bid"],how="left")\
    .select("title")\
    .filter(col("sid").isNull())\
    .show()
## Les étudiants qui ont emprunté le livre bid=’0002’
print("*** Les étudiants qui ont emprunté le livre 0002")
## SQL
spark.sql(""" SELECT sname as name
              FROM Student_SQL 
              INNER JOIN Borrow_SQL on   Borrow_SQL.sid = Student_SQL.sid  
              WHERE Borrow_SQL.bid = "0002" """)\
    .show()

## DSL
Student.join(Borrow,["sid"])\
    .select(col("sname").alias('name'))\
    .filter(col("bid")=="0002")\
    .show()

## Les titres de tous les livres empruntés par des étudiants en Mécanique
## SQL
print("**** Les titres des livres empruntés par les étudiants en mécanique")
spark.sql(""" SELECT title 
              FROM Student_SQL 
              INNER JOIN Borrow_SQL on Borrow_SQL.sid = Student_SQL.sid  
              INNER JOIN Book_SQL on Book_SQL.bid = Borrow_SQL.bid  
              WHERE Student_SQL.dept = "Mécanique" """)\
    .show() 

## DSL
Student.join(Borrow,["sid"])\
       .join(Book,["bid"])\
       .filter(col("dept")=="Mécanique")\
       .select(col("title"))\
       .show()

## Les étudiants qui n’ont jamais emprunté de livre.
## SQL
print("*** Les étudiants qui n'ont jamais emprunté de livres")
spark.sql(""" SELECT sname as name
              FROM Student_SQL 
              LEFT JOIN Borrow_SQL on   Borrow_SQL.sid = Student_SQL.sid 
              WHERE Student_SQL.sid  IS NULL """)\
    .show()

## DSL
Student.join(Borrow,["sid"],how="left")\
    .select(col("sname").alias("name"))\
    .filter(col("sid").isNull())\
    .show()

 ## Déterminer l’auteur qui a écrit le plus de livres.
 ## SQL
 print("************** L'auteur qui a écrit le plus de livres")
spark.sql(""" SELECT Author_SQL.name,COUNT(*) as nombre
              FROM Author_SQL 
              LEFT JOIN Write_SQL on   Author_SQL.aid = Write_SQL.aid 
              GROUP BY Author_SQL.name 
              ORDER BY nombre desc limit(1)""")\
    .show()
 ## DSL
Author.join(Write, ["aid"])\
      .distinct()\
      .groupBy("name")\
      .agg(F.count("bid").alias("nombre"))\
      .sort(F.col("nombre").desc())\
      .select(F.first("name").alias("auteur"),F.first("nombre").alias("nombre"))\
       .show()

## Déterminer les personnes qui n’ont pas encore rendu les livres
## SQL
print("****** Les étudiants qui n'ont pas encore rendu les livres")
spark.sql(""" SELECT sname  as name
              FROM Student_SQL   
              INNER JOIN Borrow_SQL  on   Borrow_SQL.sid = Student_SQL.sid  
              WHERE Borrow_SQL.return_time = "null" """)\
      .show()
## DSL
Student.join(Borrow,["sid"])\
    .select(col("sname").alias('name'))\
    .filter(col("return_time")=="null")\
    .show()
## Créer une nouvelle colonne dans la table borrow qui prend la valeur 1, si la durée d'emprunt est supérieur à 3 mois,  sinon 0
## SQL
print("**** Etat des emprunts")
spark.sql(""" select *, 
    case
        when (months_between(to_date(`return_time`, 'dd-MM-yyyy'),
                   to_date(`checkout_time`, 'dd-MM-yyyy')) > 3) then 1 else 0
    end as `Plus de 3 mois`
    from Borrow_SQL """).show()
## DSL
Borrow_export = Borrow\
    .withColumn("format",F.lit("dd-MM-yyyy"))\ 
    .withColumn("start",F.expr("to_date(`checkout_time`, format)"))\
    .withColumn("end",F.expr("to_date(`return_time`, format)"))\
    .withColumn("Plus de 3 mois", 
                F.when(F.months_between(F.col('end'),F.col('start')) > 3,1)\
                .otherwise(0))\
    .drop('format','start','end')

## configuration des paths
import configparser
config = configparser.ConfigParser()
config.read('properties.conf')
path_to_output_data= config['BDA']['path']

## Exportation en csv
Borrow_export.toPandas().to_csv(path_to_output_data + "Borrow.csv")

## Déterminer les livres qui n’ont jamais été empruntés
## SQL
print("*** Les livres jamais empruntés")
spark.sql(""" SELECT Book_SQL.bid, title , category 
              FROM Borrow_SQL  
              FULL JOIN Book_SQL on Borrow_SQL.bid=Book_SQL.bid 
              WHERE Borrow_SQL.bid IS NULL """  )\
      .show()
## DSL
Book.join(Borrow,["bid"],how='full')\
    .select(col("bid"),col("title"),col("category"))\
    .filter(Borrow.bid.isNull())\
    .show()





