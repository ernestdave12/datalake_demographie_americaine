import pandas as pd
import os
import glob

# -------- CONFIG --------
# Répertoire contenant les fichiers employment_2021.csv, employment_2022.csv, etc.
data_dir = "E:/Ecole/M2/Architecture_big_data/TP_GROUPE/datalake_demographie_americaine/Data_Source_1/Data Source/employment"

# Répertoire tmp (au même niveau que script/)
script_dir = os.path.dirname(os.path.abspath(__file__))
tmp_dir = os.path.join(script_dir, '..', 'tmp')
os.makedirs(tmp_dir, exist_ok=True)

# -------- GROUPS --------
groups = {
    "employment_status": [
        "Population 16 years and over",
        "In labor force",
        "Civilian labor force",
        "Employed",
        "Unemployed",
        "Armed Forces",
        "Not in labor force",
        "Unemployment Rate",
        "Females 16 years and over",
        "All parents in family in labor force"
    ],
    "commuting_to_work": [
        "Workers 16 years and over",
        "Car, truck, or van -- drove alone",
        "Car, truck, or van -- carpooled",
        "Public transportation (excluding taxicab)",
        "Walked",
        "Other means",
        "Worked from home",
        "Mean travel time to work (minutes)"
    ],
    "occupation": [
        "Civilian employed population 16 years and over",
        "Management, business, science, and arts occupations",
        "Service occupations",
        "Sales and office occupations",
        "Natural resources, construction, and maintenance occupations",
        "Production, transportation, and material moving occupations"
    ],
    "industry": [
        "Civilian employed population 16 years and over",
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
        "Other services, except public administration",
        "Public administration"
    ],
    "class_of_worker": [
        "Civilian employed population 16 years and over",
        "Private wage and salary workers",
        "Government workers",
        "Self-employed in own not incorporated business workers",
        "Unpaid family workers"
    ],
    "income_and_benefits": [
        "Total households",
        "Less than $10,000",
        "$10,000 to $14,999",
        "$15,000 to $24,999",
        "$25,000 to $34,999",
        "$35,000 to $49,999",
        "$50,000 to $74,999",
        "$75,000 to $99,999",
        "$100,000 to $149,999",
        "$150,000 to $199,999",
        "$200,000 or more",
        "Median household income (dollars)",
        "Mean household income (dollars)"
    ],
    "health_insurance": [
        "Civilian noninstitutionalized population",
        "With health insurance coverage",
        "With private health insurance",
        "With public coverage",
        "No health insurance coverage"
    ],
    "poverty": [
        "All families",
        "With related children of the householder under 18 years",
        "Married couple families",
        "Families with female householder, no spouse present",
        "All people",
        "Under 18 years",
        "18 years and over",
        "People in families",
        "Unrelated individuals 15 years and over"
    ]
}

# -------- Labels de résumé à ignorer --------
summary_labels = {
    "occupation": ["Civilian employed population 16 years and over"],
    "industry": ["Civilian employed population 16 years and over"],
    "class_of_worker": ["Civilian employed population 16 years and over"],
    "employment_status": ["Population 16 years and over"],
    "commuting_to_work": ["Workers 16 years and over"],
}

# -------- FONCTION DE TRANSFORMATION --------
def process_group(df, group_name, group_labels, year_id, state_cols):
    sub_df = df[df['Label (Grouping)'].isin(group_labels)].copy()

    # Supprimer les lignes de résumé définies
    if group_name in summary_labels:
        sub_df = sub_df[~sub_df['Label (Grouping)'].isin(summary_labels[group_name])]

    records = []
    for _, row in sub_df.iterrows():
        for state in state_cols:
            records.append({
                'state_id': state,
                'year_id': year_id,
                'indicator': row['Label (Grouping)'],
                'Estimate': row.get(f'{state}!!Estimate', None),
                'Percent': row.get(f'{state}!!Percent', None)
            })
    return pd.DataFrame(records)


# -------- TRAITEMENT DE TOUS LES FICHIERS --------
files = glob.glob(os.path.join(data_dir, "employment_*.csv"))

# Dictionnaire pour stocker toutes les années concaténées
all_tables = {group: [] for group in groups.keys()}

for file_path in files:
    # Extraire year_id depuis le nom du fichier
    year_id = int(os.path.basename(file_path).split('_')[-1].split('.')[0])

    # Charger le CSV
    df = pd.read_csv(file_path, dtype=str)
    df['Label (Grouping)'] = df['Label (Grouping)'].str.strip()  # nettoyage

    # Détection des états
    state_cols = [col.split('!!')[0] for col in df.columns[1:] if 'Estimate' in col or 'Percent' in col]
    state_cols = list(dict.fromkeys(state_cols))  # supprimer doublons

    # Garder uniquement les colonnes utiles
    df = df[['Label (Grouping)'] + [c for c in df.columns if any(state in c for state in state_cols)]]

    # Générer et stocker les tables pour ce fichier
    for group_name, group_labels in groups.items():
        table_df = process_group(df, group_name, group_labels, year_id, state_cols)
        all_tables[group_name].append(table_df)

# -------- CONCATENER ET SAUVEGARDER --------
for group_name, list_dfs in all_tables.items():
    if list_dfs:  # si non vide
        final_df = pd.concat(list_dfs, ignore_index=True)
        out_file = os.path.join(tmp_dir, f'{group_name}.csv')
        final_df.to_csv(out_file, index=False)
        print(f"✅ {group_name}.csv sauvegardé avec {len(final_df)} lignes ({len(list_dfs)} années concaténées).")

print("🎉 Toutes les tables concaténées ont été générées avec succès !")
