from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "password123"))
with driver.session() as session:
    res = session.run("MATCH (n) RETURN labels(n) as label, count(n) as count")
    for r in res:
        print(f"Label: {r[0]}, Count: {r[1]}")
driver.close()
