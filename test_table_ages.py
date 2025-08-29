import pandas as pd
from sqlalchemy import create_engine, Integer, Float, String
from sqlalchemy.types import VARCHAR


def import_ages_in_sql(year):
    # Charger CSV (attention: adapte le séparateur et l’encodage si besoin)
    df = pd.read_csv(f"./Data_Source_1/Data Source/Population_profile_{year}.csv")
    # Supprime les espaces avant/après les noms de colonnes
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()


    # On récupère les états
    etats = {col.split("!!")[0] for col in df.columns if "!!" in col}


    # Liste des tranches d’âge
    ages = [
        "Under 5 years",
        "5 to 17 years",
        "18 to 24 years",
        "25 to 34 years",
        "35 to 44 years",
        "45 to 54 years",
        "55 to 64 years",
        "65 to 74 years",
        "75 years and over",
    ]
    datas = []
    # Pour chaque état et chaque ligne (chaque tranche d’âge)
    for etat in etats:
        data = {
            "etat": etat,
            "year": year,
            "Under_5_years": None,
            "5_to_17_years": None,
            "18_to_24_years": None,
            "25_to_34_years": None,
            "35_to_44_years": None,
            "45_to_54_years": None,
            "55_to_64_years": None,
            "65_to_74_years": None,
            "75_years_and_over": None,
        }

        for idx, age in enumerate(ages):
            value = float(df.loc[df["Label (Grouping)"] == age, f"{etat}!!Total population!!Estimate"].iloc[0].replace("%", ""))

            col_name = age.replace(" ", "_").replace("-", "_")
            if age == "Under 5 years":
                data["Under_5_years"] = value
            elif age == "5 to 17 years":
                data["5_to_17_years"] = value
            elif age == "18 to 24 years":
                data["18_to_24_years"] = value
            elif age == "25 to 34 years":
                data["25_to_34_years"] = value
            elif age == "35 to 44 years":
                data["35_to_44_years"] = value
            elif age == "45 to 54 years":
                data["45_to_54_years"] = value
            elif age == "55 to 64 years":
                data["55_to_64_years"] = value
            elif age == "65 to 74 years":
                data["65_to_74_years"] = value
            elif age == "75 years and over":
                data["75_years_and_over"] = value

        datas.append(data)


    # Transformer en DataFrame
    df = pd.DataFrame(datas)

    # Création de l'engine SQLAlchemy
    engine = create_engine(
        "mssql+pyodbc://localhost\\SQLEXPRESS/USA?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes&Trusted_Connection=yes",
        fast_executemany=True
    )

    # Définir les types SQL pour certaines colonnes
    dtype_dict = {
        'etat': VARCHAR(100),
        'year': Integer,
        'Under_5_years': Float,
        '5_to_17_years': Float,
        '18_to_24_years': Float,
        '25_to_34_years': Float,
        '35_to_44_years': Float,
        '45_to_54_years': Float,
        '55_to_64_years': Float,
        '65_to_74_years': Float,
        '75_years_and_over': Float
    }

    # Insérer le DataFrame dans SQL Server
    df.to_sql(
        "age",       # Nom de la table
        engine,
        if_exists="replace", # "replace" crée la table si elle n'existe pas
        index=False,
        dtype=dtype_dict
    )

    print(f"Données pour {year}  insérées avec succès !")
