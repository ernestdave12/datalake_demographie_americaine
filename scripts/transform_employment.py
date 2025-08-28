import pandas as pd

# Charger le CSV brut
df = pd.read_csv("/tmp/employment_raw.csv", dtype=str)

# Liste des états à extraire à partir des colonnes
state_cols = [col.split('!!')[0] for col in df.columns[1:] if 'Estimate' in col or 'Percent' in col]
state_cols = list(dict.fromkeys(state_cols))  # supprimer doublons

# Retirer les colonnes inutiles
df = df[['Label (Grouping)'] + [c for c in df.columns if any(state in c for state in state_cols)]]

# Mapping des groupes avec les lignes pertinentes
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

# Fonction pour transformer chaque groupe en table wide pivotée
def process_group(df, group_name, group_labels):
    sub_df = df[df['Label (Grouping)'].isin(group_labels)].copy()
    # Identifier Estimate et Percent
    estimates = [c for c in sub_df.columns if 'Estimate' in c]
    percents = [c for c in sub_df.columns if 'Percent' in c]

    # Créer un DataFrame final par table
    records = []
    for idx, row in sub_df.iterrows():
        for state in state_cols:
            records.append({
                'state_id': state,
                'year_id': 2023,  # à adapter selon l'année de ton dataset
                'indicator': row['Label (Grouping)'],
                'Estimate': row.get(f'{state}!!Estimate', None),
                'Percent': row.get(f'{state}!!Percent', None)
            })
    return pd.DataFrame(records)

# Générer les tables pour tous les groupes
tables = {}
for group_name, group_labels in groups.items():
    tables[group_name] = process_group(df, group_name, group_labels)

# Sauvegarde en CSV séparé
for group_name, table_df in tables.items():
    table_df.to_csv(f'/tmp/{group_name}.csv', index=False)

print("Tables générées pour tous les groupes avec state_id et year_id !")
