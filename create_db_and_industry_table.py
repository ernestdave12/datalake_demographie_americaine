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

# Se reconnecter sur la DB USA
conn.close()
conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=USA;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;",     autocommit=True
)
cursor = conn.cursor()

# Supprimer la table si elle existe déjà
cursor.execute("IF OBJECT_ID('industry', 'U') IS NOT NULL DROP TABLE industry;")

# Créer la table industry
cursor.execute("""
CREATE TABLE industry (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) NOT NULL
);
""")

# Liste des secteurs
industries = [
    "Agriculture, forestry, fishing and hunting, and mining",
    "Construction",
    "Manufacturing",
    "Wholesale trade",
    "Retail trade",
    "Transportation and warehousing, and utilities",
    "Information",
    "Finance and insurance, and real estate and rental and leasing",
    "Professional, scientific, and management, and administrative and waste management services",
    "Educational services, and health care and social assistance",
    "Arts, entertainment, and recreation, and accommodation and food services",
    "Other services (except public administration)",
    "Public administration"
]

# Insertion des lignes
for industry in industries:
    cursor.execute("INSERT INTO industry (name) VALUES (?)", industry)

conn.commit()
cursor.close()
conn.close()

print("✅ Base 'USA' et table 'industry' créées avec succès !")
import pyodbc

# Connexion au serveur SQL Server (on se connecte à master pour créer la DB USA)
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"   # adapte selon ton serveur (ex: "localhost,1433")
    "DATABASE=master;"
    "UID=mon_user;"
    "PWD=mon_motdepasse;"
)
cursor = conn.cursor()

# Création de la DB USA si elle n'existe pas
cursor.execute("IF DB_ID('USA') IS NULL CREATE DATABASE USA;")
conn.commit()

# Se reconnecter sur la DB USA
conn.close()
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=USA;"
    "UID=mon_user;"
    "PWD=mon_motdepasse;"
)
cursor = conn.cursor()

# Supprimer la table si elle existe déjà
cursor.execute("IF OBJECT_ID('industry', 'U') IS NOT NULL DROP TABLE industry;")

# Créer la table industry
cursor.execute("""
CREATE TABLE industry (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) NOT NULL
);
""")

# Liste des secteurs
industries = [
    "Agriculture, forestry, fishing and hunting, and mining",
    "Construction",
    "Manufacturing",
    "Wholesale trade",
    "Retail trade",
    "Transportation and warehousing, and utilities",
    "Information",
    "Finance and insurance, and real estate and rental and leasing",
    "Professional, scientific, and management, and administrative and waste management services",
    "Educational services, and health care and social assistance",
    "Arts, entertainment, and recreation, and accommodation and food services",
    "Other services (except public administration)",
    "Public administration"
]

# Insertion des lignes
for industry in industries:
    cursor.execute("INSERT INTO industry (name) VALUES (?)", industry)

conn.commit()
cursor.close()
conn.close()

print("✅ Base 'USA' et table 'industry' créées avec succès !")
