import pyodbc

# Connexion au serveur SQL Server (on se connecte à master pour créer la DB USA)
conn = pyodbc.connect( "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=master;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;",     autocommit=True
)

cursor = conn.cursor()

# Création de la DB USA si elle n'existe pas
cursor.execute("IF DB_ID('USA') IS NULL CREATE DATABASE USA;")
conn.commit()
