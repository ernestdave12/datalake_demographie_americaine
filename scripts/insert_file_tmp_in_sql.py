import pandas as pd
from sqlalchemy import create_engine, Integer, Float, String, MetaData, Table, Column
import clean_csv
from dotenv import load_dotenv
import os
import create_and_insert_table_ages

load_dotenv()




def insert(filename):
    df = pd.read_csv(f"../tmp/{filename}.csv")

    df = clean_csv.clean(df)

    # Connexion SQLAlchemy
    engine = create_engine( "mssql+pyodbc://localhost\\SQLEXPRESS/USA?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&Trusted_Connection=yes",
                           fast_executemany=True
                           )

    # Insérer le DataFrame dans SQL Server
    df.to_sql(
        f"{filename}",  # nom de la table
        engine,
        if_exists="append",  # "append" pour insérer sans recréer la table
        index=False,
        dtype={  # schéma SQL
            "state": String(100),
            "year": Integer,
            "indicator": String(255),
            "estimate": Float,
            "percent": Float
        }
    )

    print(f"✅ Données insérées avec succès dans la table {filename} !")


insert("class_of_worker")
insert("commuting_to_work")
insert("employment_status")
insert("health_insurance")
insert("income_and_benefits")
insert("industry")
insert("occupation")
insert("poverty")
import_ages_in_sql(2021)
import_ages_in_sql(2022)
import_ages_in_sql(2023)
