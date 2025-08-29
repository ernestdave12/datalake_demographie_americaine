import pandas as pd
import pyodbc
import numpy as np
import clean_csv


def insert_income_distribution():
    # -----------------------
    # Paramètres
    # -----------------------

    # Connexion à SQL Server (adapter à ton environnement)
    conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
                          "SERVER=localhost\\SQLEXPRESS;"
                          "DATABASE=USA;"
                          "Trusted_Connection=yes;"
                          "TrustServerCertificate=yes;", autocommit=True)
    cursor = conn.cursor()

    # -----------------------
    # 1. Lecture du CSV brut
    # -----------------------
    df = pd.read_csv("../tmp/income_distribution.csv")


    # -----------------------
    # 2. Nettoyage
    # -----------------------
    df = df[df["indicator"] != "Total"]

    df.columns = df.columns.str.replace(r'[^A-Za-z0-9]+', '_', regex=True)

    for col in df.columns:
        if col not in ["state_id", "year_id", "indicator"]:
            try:
                df[col] = df[col].apply(clean_csv.clean_estimate)
            except:
                df[col] = df[col].apply(clean_csv.clean_percent)
    # -----------------------
    # 3. Création de la table
    # -----------------------
    create_table_sql = """
    IF OBJECT_ID('dbo.HouseholdIncomeDistribution', 'U') IS NOT NULL
        DROP TABLE dbo.HouseholdIncomeDistribution;

    CREATE TABLE dbo.HouseholdIncomeDistribution (
        state_id NVARCHAR(100),
        year_id INT,
        indicator NVARCHAR(200),
        Households_Estimate FLOAT NULL,
        Families_Estimate FLOAT NULL,
        Married_couple_families_Estimate FLOAT NULL,
        Nonfamily_households_Estimate FLOAT NULL
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

    # -----------------------
    # 4. Insertion des données
    # -----------------------
    insert_sql = """
    INSERT INTO dbo.HouseholdIncomeDistribution
    (state_id, year_id, indicator, Households_Estimate, Families_Estimate, Married_couple_families_Estimate, Nonfamily_households_Estimate)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        values = [None if pd.isna(v) else v for v in row]
        cursor.execute(insert_sql, tuple(values))

    conn.commit()

    print("✅ Données nettoyées et insérées dans SQL Server avec succès !")

    cursor.close()
    conn.close()
def insert_income_percent_allocated():

    conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
                          "SERVER=localhost\\SQLEXPRESS;"
                          "DATABASE=USA;"
                          "Trusted_Connection=yes;"
                          "TrustServerCertificate=yes;", autocommit=True)
    cursor = conn.cursor()

    # -----------------------
    # 1. Lecture du CSV brut
    # -----------------------
    df = pd.read_csv("../tmp/income_percent_allocated.csv")

    # -----------------------
    # 2. Nettoyage
    # -----------------------
    df.columns = df.columns.str.replace(r'[^A-Za-z0-9]+', '_', regex=True)

    for col in df.columns:
        if col not in ["state_id", "year_id", "indicator"]:
            try:
                df[col] = df[col].apply(clean_csv.clean_estimate)
            except:
                df[col] = df[col].apply(clean_csv.clean_percent)

    # -----------------------
    # 3. Création de la table
    # -----------------------
    create_table_sql = """
    IF OBJECT_ID('dbo.HouseholdIncomeStats', 'U') IS NOT NULL
        DROP TABLE dbo.HouseholdIncomeStats;

    CREATE TABLE dbo.HouseholdIncomeStats (
        state_id NVARCHAR(100),
        year_id INT,
        indicator NVARCHAR(100),
        Households_Estimate FLOAT,
        Families_Estimate FLOAT NULL,
        Married_couple_families_Estimate FLOAT NULL,
        Nonfamily_households_Estimate FLOAT NULL
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

    # -----------------------
    # 4. Insertion des données
    # -----------------------
    insert_sql = """
    INSERT INTO dbo.HouseholdIncomeStats
    (state_id, year_id, indicator, Households_Estimate, Families_Estimate, Married_couple_families_Estimate, Nonfamily_households_Estimate)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        values = [None if pd.isna(v) else v for v in row]
        cursor.execute(insert_sql, tuple(values))

    conn.commit()

    print("✅ Données nettoyées et insérées dans SQL Server avec succès !")

    cursor.close()
    conn.close()


insert_income_distribution()
insert_income_percent_allocated()