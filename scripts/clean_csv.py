import pandas as pd

def clean(df):
    # Renommer les colonnes
    df = df.rename(columns={
        "state_id": "state",
        "year_id": "year",
        "Estimate": "estimate",
        "Percent": "percent"
    })

    # Nettoyage des colonnes
    def clean_estimate(val):
        if pd.isna(val) or str(val).strip() in ["(X)", "X"]:
            return None
        try:
            return float(str(val).replace(",", "").strip())
        except ValueError:
            return None  # si vraiment pas convertible

    def clean_percent(val):
        if pd.isna(val) or str(val).strip() in ["(X)", "X"]:
            return None
        try:
            return float(str(val).replace("%", "").strip())
        except ValueError:
            return None

    df["estimate"] = df["estimate"].apply(clean_estimate)
    df["percent"] = df["percent"].apply(clean_percent)

    return df
