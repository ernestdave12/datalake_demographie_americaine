# -*- coding: utf-8 -*-
import pandas as pd
import os
import glob
import re
from create_tables import make_engine_trusted


def load_education_files(folder: str, pattern: str = "education_*.csv") -> pd.DataFrame:
    """
    Purpose:
        Read all CSV files matching a pattern from a folder, add the year from
        the filename (education_YYYY.csv), and return one combined DataFrame.

    Parameters:
        folder (str): Path to the folder containing the CSV files.
        pattern (str): Glob pattern to match files (default: 'education_*.csv').

    Returns:
        pd.DataFrame: Concatenated DataFrame with a 'year' column.
    """

    if not os.path.isdir(folder):
        raise FileNotFoundError(f"Directory not found: {folder}")

    files = sorted(glob.glob(os.path.join(folder, pattern)))
    if not files:
        raise FileNotFoundError(f"No files found matching '{pattern}' in {folder}")

    frames = []
    for path in files:
        try:
            df = pd.read_csv(path)
            # Extract year from filename 'education_YYYY.csv'
            try:
                name = os.path.splitext(os.path.basename(path))[0]
                year_str = name.split("_")[-1]
                df["year"] = int(year_str)
            except Exception:
                df["year"] = None
            frames.append(df)
        except Exception as e:
            print(f"Skipping file due to read error: {path} ({e})")

    if not frames:
        raise ValueError("All matching files failed to load.")

    return pd.concat(frames, ignore_index=True)

def build_dim_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Purpose:
        Extract unique state names from wide columns and return a dim_state
        table with a surrogate key index (state_id).

    Parameters:
        df (pd.DataFrame): Input DataFrame whose columns include patterns like
                           'United States!!Total!!Estimate'. Also contains
                           'Label (Grouping)' and 'year' columns to ignore.

    Returns:
        pd.DataFrame: DataFrame with index 'state_id' (starting at 1) and
                      one column 'state_name'.
    """
    states = []
    for col in df.columns:
        if col in ["Label (Grouping)", "year"]:
            continue
        state = col.split("!!")[0]
        if state not in states:
            states.append(state)

    dim_state = pd.DataFrame({"state_name": sorted(states)})
    dim_state.index = range(1, len(dim_state) + 1)
    dim_state.index.name = "state_id"
    return dim_state

def build_dim_education(df: pd.DataFrame) -> pd.DataFrame:
    """
    Purpose:
        Extract unique education level names from the 'Label (Grouping)' column
        and return a dim_education_level table with a surrogate key index.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing 'Label (Grouping)'.

    Returns:
        pd.DataFrame: Index 'education_level_id' (starting at 1) and
                      one column 'education_level_name'.
    """
    labels = df["Label (Grouping)"].astype(str).str.strip()
    # Keep rows that look like education levels (simple keyword filter)
    mask = labels.str.contains(
        r"(graduate|diploma|college|associate|bachelor|professional|degree|9th|12th)",
        case=False, regex=True
    )
    levels = sorted(labels[mask].drop_duplicates())
    dim_edu = pd.DataFrame({"education_level_name": levels})
    dim_edu.index = range(1, len(dim_edu) + 1)
    dim_edu.index.name = "education_level_id"
    return dim_edu

def build_dim_age(df: pd.DataFrame) -> pd.DataFrame:
    """
    Purpose:
        Extract unique age group names from education and population profile files
        and return a dim_age_group table

    Parameters:
        df (pd.DataFrame): Input DataFrame containing 'age_group'.

    Returns:
        pd.DataFrame: one column 'age_group_name'
    """
    ages = [
    "Under 5 years",
    "5 to 17 years",
    "18 to 24 years",
    "25 years and over",
    "25 to 34 years",
    "35 to 44 years",
    "45 to 64 years"
    "45 to 54 years",
    "55 to 64 years",
    "65 to 74 years",
    "65 years and over",
    "75 years and over",
]
    dim_age = pd.DataFrame({"age_group": ages})
    return dim_age

def clean(x: str) -> str:
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).replace("\xa0"," ")).strip()



def split_metric_col(col: str):
    # attend: "<State>!!<Category>!!Estimate"
    parts = col.split("!!")
    if len(parts) >= 3 and parts[-1] == "Estimate":
        state = parts[0].strip()
        cat = "!!".join(parts[1:-1]).strip().lower()
        cat = re.sub(r"\s+", " ", cat)
        if cat in metric_map:
            return state, metric_map[cat]
    return None

def convert_numeric(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    # Remplacer (X) -> NA
    for col in ["total_percent","male_percent","female_percent",
                "total_estimate","male_estimate","female_estimate"]:
        if col in df.columns:
            df[col] = df[col].replace("(X)", pd.NA)

    # % -> float [0..1]
    for col in ["total_percent","male_percent","female_percent"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "", regex=False).str.rstrip("%")
            df[col] = pd.to_numeric(df[col], errors="coerce") / 100

    # effectifs -> Int64
    for col in ["total_estimate","male_estimate","female_estimate"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def build_fact_age_by_education(df: pd.DataFrame) -> pd.DataFrame:
    """
    Purpose:
        Build the fact table for age by education level.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing education and age group information.

    Returns:
        pd.DataFrame: Fact table with age and education level metrics.
    """
    # -------- construire age_group (par année)
    age_groups = [
        "Population 18 to 24 years",
        "Population 25 years and over",
        "Population 25 to 34 years",
        "Population 35 to 44 years",
        "Population 45 to 64 years",
        "Population 65 years and over",
    ]
    df["age_group"] = df["label_clean"].where(df["label_clean"].isin(age_groups))
    df["age_group"] = df.groupby("year", group_keys=False)["age_group"].ffill()

    # -------- supprimer entêtes
    drop_labels = ["AGE BY EDUCATIONAL ATTAINMENT"] + age_groups
    base = df[~df["label_clean"].isin(drop_labels)].copy()
    base["education"] = base["label_clean"]

    # -------- sélectionner colonnes métriques
    selected_cols, tuples = [], []
    for c in base.columns:
        if "!!" in c:
            info = split_metric_col(c)
            if info is not None:
                selected_cols.append(c)
                tuples.append(info)

    if not selected_cols:
        raise RuntimeError("Aucune colonne métrique au format '<State>!!<Category>!!Estimate' n'a été trouvée.")

    metrics_df = base[selected_cols].copy()
    mi = pd.MultiIndex.from_tuples(tuples, names=["state", "metric"])
    metrics_df.columns = mi

    # -------- passer en long sur 'state' et recombiner
    stacked = metrics_df.stack(level=0).reset_index(names=["row_id", "state"])
    meta = base[["year", "education", "age_group"]].reset_index(names="row_id")
    data = meta.merge(stacked, on="row_id", how="right").drop(columns=["row_id"])

    # -------- ordonner & typer (10 colonnes)
    cols_age = [
        "state", "year",
        "education",
        "age_group",
        "total_estimate",
        "total_percent",
        "male_estimate",
        "male_percent",
        "female_estimate",
        "female_percent",
    ]
    for c in cols_age:
        if c not in data.columns:
            data[c] = pd.NA

    data = data[cols_age]
    data["year"] = pd.to_numeric(data["year"], errors="coerce").astype("Int64")
    data = convert_numeric(data)


    data['age_group'] = data['age_group'].replace(age_group_mapping)

    return data

def build_fact_earning_by_education(df: pd.DataFrame) -> pd.DataFrame:
    """
    Purpose:
        Build the fact table for earnings by education level.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing education and earnings information.
        dim_age (pd.DataFrame): Dimension table for age groups.
        dim_education (pd.DataFrame): Dimension table for education levels.

    Returns:
        pd.DataFrame: Fact table with earnings and education level metrics.
    """
    
    # ---------- délimitation de la section "MEDIAN EARNINGS..."
    SECTION_HDR = "MEDIAN EARNINGS IN THE PAST 12 MONTHS (IN 2023 INFLATION-ADJUSTED DOLLARS)"
    AGE_HDR = "Population 25 years and over with earnings"
    EDU_ROWS = [
        "Less than high school graduate",
        "High school graduate (includes equivalency)",
        "Some college or associate's degree",
        "Bachelor's degree",
        "Graduate or professional degree",
    ]

    # baliser la section par année
    df["section"] = df["label_clean"].where(df["label_clean"] == SECTION_HDR)
    df["section"] = df.groupby("year", group_keys=False)["section"].ffill()

    sec = df[df["section"] == SECTION_HDR].copy()

    # construire age_group (uniquement l'en-tête demandé)
    sec["age_group"] = sec["label_clean"].where(sec["label_clean"] == AGE_HDR)
    sec["age_group"] = sec.groupby("year", group_keys=False)["age_group"].ffill()

    # ne garder que les lignes d'éducation sous cet age_group
    base = sec[
        sec["label_clean"].isin(EDU_ROWS) & (sec["age_group"] == AGE_HDR)
    ].copy()
    base["education"] = base["label_clean"]

    # ---------- sélectionner colonnes métriques (états)
    selected_cols, tuples = [], []
    for c in base.columns:
        if "!!" in c:
            info = split_metric_col(c)
            if info is not None:
                selected_cols.append(c)
                tuples.append(info)

    if not selected_cols:
        raise RuntimeError(
            "Aucune colonne métrique au format '<State>!!<Category>!!Estimate' n'a été trouvée."
        )

    metrics_df = base[selected_cols].copy()
    mi = pd.MultiIndex.from_tuples(tuples, names=["state", "metric"])
    metrics_df.columns = mi

    # ---------- passage en long sur 'state' et recombinaison
    stacked = metrics_df.stack(level=0).reset_index(names=["row_id", "state"])
    meta = base[["year", "education", "age_group"]].reset_index(names="row_id")
    earning_by_education = meta.merge(stacked, on="row_id", how="right").drop(columns=["row_id"])

    # ---------- ordonner & typer (10 colonnes)
    cols = [
        "state", "year",
        "education",
        "age_group",
        "total_estimate",
        "male_estimate",
        "female_estimate"
    ]
    for c in cols:
        if c not in earning_by_education.columns:
            earning_by_education[c] = pd.NA

    earning_by_education = earning_by_education[cols]
    earning_by_education["year"] = pd.to_numeric(earning_by_education["year"], errors="coerce").astype("Int64")
    earning_by_education = convert_numeric(earning_by_education)

    earning_by_education['age_group'] = earning_by_education['age_group'].replace(age_group_mapping)
    earning_by_education.to_csv("earning_by_education.csv", index=False)
    return earning_by_education


df = load_education_files("education")

if __name__ == "__main__":
    # Charger les données
    df = load_education_files("education")

    # Générer les dimensions
    dim_state = build_dim_state(df)
    dim_education = build_dim_education(df)
    dim_age = build_dim_age(df)

    # Générer les facts
    metric_map = {
    "total": "total_estimate",
    "percent": "total_percent",
    "male": "male_estimate",
    "percent male": "male_percent",
    "female": "female_estimate",
    "percent female": "female_percent",
    }

    age_group_mapping = {
        "Population 18 to 24 years": "18 to 24 years",
        "Population 25 years and over":  "25 years and over",
        "Population 25 to 34 years": "25 to 34 years",
        "Population 35 to 44 years": "35 to 44 years",
        "Population 45 to 64 years": "45 to 64 years",
        "Population 65 years and over": "65 years and over",
        "Population 25 years and over with earnings": "25 years and over"
    }
    label_col = "Label (Grouping)"
    df["label_clean"] = df[label_col].map(clean)

    fact_age_by_education = build_fact_age_by_education(df)
    fact_earning_by_education = build_fact_earning_by_education(df)

    # Sauvegarder les DataFrames en CSV
    # dim_state.to_csv("dim_state.csv")
    # dim_education.to_csv("dim_education.csv")
    # dim_age.to_csv("dim_age.csv")
    # fact_age_by_education.to_csv("fact_age_by_education.csv", index=False)
    # fact_earning_by_education.to_csv("fact_earning_by_education.csv", index=False)
    # #####
    # Ecriture dans la db
    SERVER   = r"NEOS-NBK1158\SQLEXPRESS"
    DATABASE = "USA"

    engine = make_engine_trusted(SERVER, DATABASE)
    dim_state.to_sql(
        "state",
        con=engine,
        schema="dbo",
        if_exists="replace",
        index=True,
        chunksize=10000,
        method=None
    )

    dim_education.to_sql(
        "education",
        con=engine,
        schema="dbo",
        if_exists="replace",
        index=True,
        chunksize=10000,
        method=None
    )

    dim_age.to_sql(
        "age",
        con=engine,
        schema="dbo",
        if_exists="replace",
        index=False,
        chunksize=10000,
        method=None
    )

    fact_age_by_education.to_sql(
        "age_by_education",
        con=engine,
        schema="dbo",
        if_exists="replace",
        index=False,
        chunksize=10000,
        method=None
    )

    fact_earning_by_education.to_sql(
        "earning_by_education",
        con=engine,
        schema="dbo",
        if_exists="replace",
        index=False,
        chunksize=10000,
        method=None
    )