import pandas as pd
from sqlalchemy import create_engine, Integer, Float, String, MetaData, Table, Column


df = pd.read_csv("../tmp/industry.csv")

# Renommer les colonnes
df = df.rename(columns={
    "state_id": "state",
    "year_id": "year",
    "Estimate": "estimate",
    "Percent": "percent"
})

# Nettoyer "estimate" → int
df["estimate"] = df["estimate"].str.replace(",", "", regex=False).astype(int)

# Nettoyer "percent" → float
df["percent"] = df["percent"].str.replace("%", "", regex=False).astype(float)


# Connexion SQLAlchemy
engine = create_engine(
    "mssql+pyodbc://localhost\\SQLEXPRESS/USA?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&Trusted_Connection=yes",
    fast_executemany=True
)

metadata = MetaData()

# Définition de la table
industry = Table(
    "industry", metadata,
    Column("state", String(100), nullable=False),
    Column("year", Integer, nullable=False),
    Column("indicator", String(255), nullable=False),
    Column("estimate", Integer),
    Column("percent", Float)
)

# Crée la table si elle n’existe pas
metadata.create_all(engine)

# Insérer le DataFrame
df.to_sql("industry", engine, if_exists="append", index=False)

