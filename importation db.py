import csv
import sqlite3
import os
import re

# dossier contenant les CSV
dossier_source = r"C:/Users/antoi/Downloads/agoun/Sae"

# base SQLite
db_path = os.path.join(dossier_source, "SAE.db")


def clean_int(text):
    if not text or text.strip() == "":
        return 0
    clean = re.sub(r"[^\d]", "", text)
    return int(clean) if clean else 0


def fix_encoding(text):
    if not text:
        return text
    try:
        return text.encode("latin1").decode("utf-8")
    except:
        return text


def vider_tables(cursor):

    cursor.execute("DELETE FROM Enregistrer")
    cursor.execute("DELETE FROM Zonage")
    cursor.execute("DELETE FROM Crime")
    cursor.execute("DELETE FROM Service")
    cursor.execute("DELETE FROM Departement")


def charger_donnees():

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("Connexion à la base :", db_path)

    vider_tables(cursor)
    conn.commit()

    print("Anciennes données supprimées")

    # contrainte unique pour éviter doublons zonage
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS zonage_unique
    ON Zonage(csp, code_departement)
    """)

    fichiers = [f for f in os.listdir(dossier_source) if f.lower().endswith(".csv")]

    print("Fichiers CSV trouvés :", len(fichiers))

    for nom_fichier in fichiers:

        chemin = os.path.join(dossier_source, nom_fichier)

        print("\nTraitement :", nom_fichier)

        try:
            with open(chemin, encoding="utf-8-sig") as f:
                reader = list(csv.reader(f, delimiter=";"))
        except:
            with open(chemin, encoding="cp1252") as f:
                reader = list(csv.reader(f, delimiter=";"))

        if not reader:
            continue

        header = reader[0][0]

        # année
        match = re.search(r"\d{4}", header)
        if not match:
            continue

        annee = int(match.group())

        # service
        if "police" in header.lower():
            service = "Police nationale"
        else:
            service = "Gendarmerie nationale"

        cursor.execute(
            "INSERT OR IGNORE INTO Service(service) VALUES (?)",
            (service,)
        )

        # structure différente PN / GN
        if service == "Police nationale":
            idx_unite = 2
            idx_data = 3
            idx_perimetre = 1
        else:
            idx_unite = 1
            idx_data = 2
            idx_perimetre = None

        mapping = {}

        # lecture colonnes (zonage)
        for col in range(2, len(reader[0])):

            code_dept = reader[0][col].strip()
            csp = fix_encoding(reader[idx_unite][col].strip())

            if not csp:
                continue

            if idx_perimetre:
                perimetre = fix_encoding(reader[idx_perimetre][col].strip())
            else:
                perimetre = "GN"

            cursor.execute(
                "INSERT OR IGNORE INTO Departement(code_departement) VALUES (?)",
                (code_dept,)
            )

            cursor.execute(
                """
                INSERT OR IGNORE INTO Zonage(csp, perimetre, code_departement, service)
                VALUES (?, ?, ?, ?)
                """,
                (csp, perimetre, code_dept, service)
            )

            cursor.execute(
                """
                SELECT id FROM Zonage
                WHERE csp=? AND code_departement=? AND service=?
                """,
                (csp, code_dept, service)
            )

            res = cursor.fetchone()

            if res:
                mapping[col] = res[0]

        count = 0

        # lecture crimes
        for row in reader[idx_data:]:

            if not row or not row[0].isdigit():
                continue

            code_index = int(row[0])
            libelle = fix_encoding(row[1])

            # insertion crime
            cursor.execute(
                """
                INSERT OR IGNORE INTO Crime(code_index, libelle_index)
                VALUES (?, ?)
                """,
                (code_index, libelle)
            )

            for col, id_zonage in mapping.items():

                if col < len(row):

                    nb = clean_int(row[col])

                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO Enregistrer
                        (id, code_index, annee, nombre_faits)
                        VALUES (?, ?, ?, ?)
                        """,
                        (id_zonage, code_index, annee, nb)
                    )

                    count += 1

        conn.commit()

        print("Statistiques insérées :", count)

    conn.close()

    print("\nIMPORT TERMINÉ")


charger_donnees()