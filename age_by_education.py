# -*- coding: utf-8 -*-
import pandas as pd
import re

df = pd.read_csv("education/education_2023.csv")

# --- 1) Normaliser le label & construire age_group ---
def clean(x):
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).replace("\xa0"," ")).strip()

df["label_clean"] = df["Label (Grouping)"].map(clean)

age_groups = [
    "Population 18 to 24 years",
    "Population 25 years and over",
    "Population 25 to 34 years",
    "Population 35 to 44 years",
    "Population 45 to 64 years",
    "Population 65 years and over",
]

# age_group: remplir vers l'avant à partir des en-têtes d'âges
df["age_group"] = df["label_clean"].where(df["label_clean"].isin(age_groups))
df["age_group"] = df["age_group"].ffill()

# Exclure l'en-tête global + les lignes d'en-tête d'âges
drop_labels = ["AGE BY EDUCATIONAL ATTAINMENT"] + age_groups
base = df[~df["label_clean"].isin(drop_labels)].copy()

# education = libellé détaillé
base["education"] = base["label_clean"]

# --- 2) Sélectionner et mapper les colonnes metrics ---
# On ne garde que les colonnes de type "<State>!!<Category>!!Estimate"
metric_map = {
    "total": "total_estimate",
    "percent": "total_percent",
    "male": "male_estimate",
    "percent male": "male_percent",
    "female": "female_estimate",
    "percent female": "female_percent",
}

def split_col(col):
    parts = col.split("!!")
    if len(parts) >= 3 and parts[-1] == "Estimate":
        state = parts[0].strip()
        cat = "!!".join(parts[1:-1]).strip().lower()
        cat = re.sub(r"\s+", " ", cat)  # normaliser espaces
        if cat in metric_map:
            return state, metric_map[cat]
    return None

selected = []
tuples = []
for c in base.columns:
    if "!!" in c:
        info = split_col(c)
        if info is not None:
            selected.append(c)
            tuples.append(info)

# Si rien n'est trouvé, lever une alerte soft
if not selected:
    raise RuntimeError("Aucune colonne metrics trouvée au format '<State>!!<Category>!!Estimate'.")

# Construire un MultiIndex (state, metric)
metrics_df = base[selected].copy()
mi = pd.MultiIndex.from_tuples(tuples, names=["state", "metric"])
metrics_df.columns = mi

# --- 3) Passage en long sur l'axe 'state' puis recombinaison ---
stacked = metrics_df.stack(level=0)  # index = (row_index, state), colonnes = metrics
# le nom de la 2e dimension d'index est 'state' grâce au nom du MultiIndex
stacked = stacked.reset_index(names=["row_id", "state"])

# Récupérer education/age_group en joignant sur l'index de base
meta = base[["education", "age_group"]].reset_index(names="row_id")
tidy = meta.merge(stacked, on="row_id", how="right").drop(columns=["row_id"])

# --- 4) Réordonner & ne garder que les 9 colonnes demandées ---
cols_order = [
    "state",
    "education",
    "age_group",
    "total_estimate",
    "total_percent",
    "male_estimate",
    "male_percent",
    "female_estimate",
    "female_percent",
]
# certaines colonnes peuvent manquer pour certains états; on les ajoute vides si besoin
for c in cols_order:
    if c not in tidy.columns:
        tidy[c] = pd.NA

#Convertir les nombres en numérique
cols_to_convert = {
    "total_percent": float, "male_percent": float, "female_percent": float,
    "total_estimate": "Int64", "male_estimate": "Int64", "female_estimate": "Int64"
}

for col, dtype in cols_to_convert.items():
    if col in tidy.columns:
        # Replace (X) with NA
        tidy[col] = tidy[col].replace('(X)', pd.NA)

        # Remove commas and '%' for percentage columns
        if dtype == float:
            tidy[col] = tidy[col].astype(str).str.replace(",", "").str.rstrip("%")
            # Convert to float and divide by 100 for percentages
            tidy[col] = pd.to_numeric(tidy[col], errors='coerce') / 100
        else:
            # Convert to integer for estimate columns
            tidy[col] = tidy[col].astype(str).str.replace(",", "")
            tidy[col] = pd.to_numeric(tidy[col], errors='coerce').astype(dtype)


tidy = tidy[cols_order]

tidy.to_csv("education/education_2023_filtered.csv", index=False)
