import pandas as pd
from sqlalchemy import create_engine
import urllib
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import os

load_dotenv()

# ============ CONNEXION SQL SERVER ============
def make_engine_trusted(server: str, database: str) -> Engine:
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"  # Assurez-vous que le pilote est installé
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True, future=True)
engine = make_engine_trusted("localhost\\SQLEXPRESS", "USA")


# ============ CREATION DES TABLES ============
def create_tables(engine: Engine) -> None:
    """
    Crée les tables nécessaires dans la base de données.
    state, age, education, earning_by_education, age_by_education
    """
    # ============ DDL (DROP + CREATE) ============
    DDL = r"""
    IF OBJECT_ID('dbo.earning_by_education','U') IS NOT NULL DROP TABLE dbo.earning_by_education;
    IF OBJECT_ID('dbo.age_by_education','U')    IS NOT NULL DROP TABLE dbo.age_by_education;
    IF OBJECT_ID('dbo.education','U')           IS NOT NULL DROP TABLE dbo.education;
    IF OBJECT_ID('dbo.age','U')                 IS NOT NULL DROP TABLE dbo.age;
    IF OBJECT_ID('dbo.state','U')               IS NOT NULL DROP TABLE dbo.state;
    IF OBJECT_ID('dbo.date','U')                IS NOT NULL DROP TABLE dbo.date;

    CREATE TABLE dbo.state (
        state_name          nvarchar(100) NOT NULL
    );

    CREATE TABLE dbo.age (
        age_group           nvarchar(200) NOT NULL
    );

    CREATE TABLE dbo.date (
        id   int IDENTITY(1,1) PRIMARY KEY,
        year int NOT NULL
    );

    CREATE TABLE dbo.education (
        education_level_name nvarchar(200) NOT NULL
    );

    /* Tables de faits (10 colonnes) */
    CREATE TABLE dbo.age_by_education (
        state              nvarchar(100) NOT NULL,
        year               int           NOT NULL,
        education          nvarchar(200) NOT NULL,
        age_group          nvarchar(200) NOT NULL,
        total_estimate     int           NULL,
        total_percent      float         NULL,
        male_estimate      int           NULL,
        male_percent       float         NULL,
        female_estimate    int           NULL,
        female_percent     float         NULL
    );

    CREATE TABLE dbo.earning_by_education (
        state              nvarchar(100) NOT NULL,
        year               int           NOT NULL,
        education          nvarchar(200) NOT NULL,
        age_group          nvarchar(200) NOT NULL,
        total_estimate     int           NULL,
        total_percent      float         NULL,
        male_estimate      int           NULL,
        male_percent       float         NULL,
        female_estimate    int           NULL,
        female_percent     float         NULL
    );

    /* Index conseillés pour les requêtes typiques */
    CREATE INDEX IX_age_by_education_state_year ON dbo.age_by_education(state, year);
    CREATE INDEX IX_earning_by_education_state_year ON dbo.earning_by_education(state, year);
    """
    

    with engine.begin() as conn:
        conn.exec_driver_sql(DDL)


try:
    create_tables(engine)
    print("Tables créées avec succès.")
except Exception as e:
    print(f"Erreur lors de la création des tables : {e}")

    ##