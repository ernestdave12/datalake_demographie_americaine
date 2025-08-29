import pandas as pd
import pyodbc

# Charger ton CSV
df = pd.read_csv("ton_fichier.csv")

# Récupérer les entêtes
colonnes = df.columns.tolist()

# Extraire uniquement la partie avant "!!"
etats = [col.split("!!")[0] for col in colonnes if "!!" in col]

# Supprimer les doublons en gardant l'ordre
etats_uniques = list(dict.fromkeys(etats))


# Connexion à ta base USA
conn = pyodbc.connect( "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=USA;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;",     autocommit=True
)

cursor = conn.cursor()

# Créer la table state si elle n’existe pas
cursor.execute("""
IF OBJECT_ID('dbo.state', 'U') IS NULL
CREATE TABLE state (
    id INT IDENTITY(1,1) PRIMARY KEY,
    value NVARCHAR(255) NOT NULL
)
""")
conn.commit()

# Insérer les états
for etat in etats_uniques:
    cursor.execute("INSERT INTO state (value) VALUES (?)", etat)

conn.commit()
cursor.close()
conn.close()

print("✅ États insérés dans la table state")

