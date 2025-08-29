import pandas as pd
import os
import glob

# -------- CONFIG --------
data_dir = "E:/Ecole/M2/Architecture_big_data/TP_GROUPE/datalake_demographie_americaine/Data_Source_1/Data Source/income"

script_dir = os.path.dirname(os.path.abspath(__file__))
tmp_dir = os.path.join(script_dir, '..', 'tmp')
os.makedirs(tmp_dir, exist_ok=True)

# -------- GROUPS --------
groups = {
    "income_distribution": [
        "Total",
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
        "Median income (dollars)",
        "Mean income (dollars)"
    ],
    "income_percent_allocated": [
        "PERCENT ALLOCATED",
        "Household income in the past 12 months",
        "Family income in the past 12 months",
        "Nonfamily income in the past 12 months"
    ]
}

# -------- FONCTION DE TRANSFORMATION --------
def process_group(df, group_labels, year_id):
    # Ignorer la ligne "PERCENT ALLOCATED"
    sub_df = df[df['Label (Grouping)'].isin(group_labels) & (df['Label (Grouping)'] != "PERCENT ALLOCATED")].copy()
    records = []

    for _, row in sub_df.iterrows():
        row_dict = {}
        row_dict['indicator'] = row['Label (Grouping)']
        row_dict['year_id'] = year_id

        for col in df.columns[1:]:
            parts = col.split('!!')
            if len(parts) == 3:
                state, category, value_type = parts
                col_name = f"{category}_{value_type}"  # ex: Households_Estimate
                if state not in row_dict:
                    row_dict[state] = {}
                row_dict[state][col_name] = row[col]

        # Transformer pour chaque état en ligne séparée
        for state, values in row_dict.items():
            if state in ['indicator', 'year_id']:
                continue
            record = {'state_id': state, 'year_id': year_id, 'indicator': row_dict['indicator']}
            record.update(values)
            records.append(record)

    return pd.DataFrame(records)

# -------- TRAITEMENT DE TOUS LES FICHIERS --------
files = glob.glob(os.path.join(data_dir, "total_income_*.csv"))

all_tables = {group: [] for group in groups.keys()}

for file_path in files:
    year_id = int(os.path.basename(file_path).split('_')[-1].split('.')[0])
    df = pd.read_csv(file_path, dtype=str)
    df['Label (Grouping)'] = df['Label (Grouping)'].astype(str).str.strip()

    for group_name, group_labels in groups.items():
        table_df = process_group(df, group_labels, year_id)
        all_tables[group_name].append(table_df)

# -------- CONCATENER ET SAUVEGARDER --------
for group_name, list_dfs in all_tables.items():
    if list_dfs:
        final_df = pd.concat(list_dfs, ignore_index=True)
        out_file = os.path.join(tmp_dir, f'{group_name}.csv')
        final_df.to_csv(out_file, index=False)
        print(f"✅ {group_name}.csv sauvegardé avec {len(final_df)} lignes ({len(list_dfs)} années concaténées).")
    else:
        print(f"⚠️ Aucun data pour le groupe {group_name}, fichier non créé.")

print("🎉 Toutes les tables income ont été générées avec succès !")
