from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7688", auth=("neo4j", "password123"))
with driver.session() as session:
    res = session.run("MATCH (p:Page) WHERE p.pdf_links IS NOT NULL AND size(p.pdf_links) > 0 RETURN p.title, p.pdf_links")
    for r in res:
        print(f"Title: {r[0]}")
        print(f"Links: {r[1]}")
        print("-" * 20)
driver.close()
