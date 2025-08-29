import pyodbc

username = "root"
password = "mdp"

try:
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=10.74.2.174;"
        f"DATABASE=USA;"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )
    print("Connexion réussie avec authentification SQL !")
    conn.close()
except pyodbc.Error as e:
    print("Erreur de connexion avec authentification SQL :", e)
