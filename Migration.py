import sqlite3
from neo4j import GraphDatabase

# -------------------------
# Connexion SQLite
# -------------------------

sqlite_path = r"C:/Users/antoi/Downloads/agoun/Sae/SAE.db"

conn = sqlite3.connect(sqlite_path)
cursor = conn.cursor()

# -------------------------
# Connexion Neo4j
# -------------------------

uri = "bolt://localhost:7687"
user = "neo4j"
password = "neo4j123"   # mets ton mot de passe ici

driver = GraphDatabase.driver(uri, auth=(user, password))


def run_query(query, params=None):
    with driver.session() as session:
        session.run(query, params or {})


print("Connexion OK")


# -------------------------
# RESET NEO4J
# -------------------------

print("Suppression des anciennes données Neo4j")

run_query("""
MATCH (n)
DETACH DELETE n
""")

print("Base Neo4j vidée")


# -------------------------
# DEPARTEMENT
# -------------------------

print("Import Departement")

for row in cursor.execute("SELECT * FROM Departement"):

    run_query(
        """
        MERGE (d:Departement {code:$code})
        """,
        {"code": row[0]}
    )


# -------------------------
# SERVICE
# -------------------------

print("Import Service")

for row in cursor.execute("SELECT * FROM Service"):

    run_query(
        """
        MERGE (s:Service {nom:$nom})
        """,
        {"nom": row[0]}
    )


# -------------------------
# CRIME
# -------------------------

print("Import Crime")

for row in cursor.execute("SELECT * FROM Crime"):

    run_query(
        """
        MERGE (c:Crime {code:$code})
        SET c.libelle=$libelle
        """,
        {
            "code": row[0],
            "libelle": row[1]
        }
    )


# -------------------------
# ZONAGE
# -------------------------

print("Import Zonage")

for row in cursor.execute("SELECT * FROM Zonage"):

    id_zonage = row[0]
    csp = row[1]
    perimetre = row[2]
    dept = row[3]
    service = row[4]

    run_query(
        """
        MERGE (z:Zonage {id:$id})
        SET z.csp=$csp,
            z.perimetre=$perimetre
        """,
        {
            "id": id_zonage,
            "csp": csp,
            "perimetre": perimetre
        }
    )

    run_query(
        """
        MATCH (z:Zonage {id:$id})
        MATCH (d:Departement {code:$dept})
        MERGE (z)-[:LOCALISE]->(d)
        """,
        {
            "id": id_zonage,
            "dept": dept
        }
    )

    run_query(
        """
        MATCH (z:Zonage {id:$id})
        MATCH (s:Service {nom:$service})
        MERGE (s)-[:GERE]->(z)
        """,
        {
            "id": id_zonage,
            "service": service
        }
    )


# -------------------------
# ENREGISTRER
# -------------------------

print("Import Enregistrer")

count = 0

for row in cursor.execute("SELECT * FROM Enregistrer WHERE nombre_faits > 0"):

    id_zonage = row[0]
    code_crime = row[1]
    annee = row[2]
    faits = row[3]

    run_query(
        """
        MATCH (z:Zonage {id:$id})
        MATCH (c:Crime {code:$crime})
        CREATE (z)-[:ENREGISTRE {annee:$annee, faits:$faits}]->(c)
        """,
        {
            "id": id_zonage,
            "crime": code_crime,
            "annee": annee,
            "faits": faits
        }
    )

    count += 1

    if count % 5000 == 0:
        print("Relations créées :", count)


driver.close()
conn.close()

print("Migration terminée")