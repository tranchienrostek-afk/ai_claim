import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("neo4j_uri", "bolt://localhost:7688")
user = os.getenv("neo4j_user", "neo4j")
password = os.getenv("neo4j_password", "password123")
driver = GraphDatabase.driver(uri, auth=(user, password))

def verify():
    with driver.session() as session:
        # Check Protocol
        protocol_count = session.run("MATCH (p:Protocol) RETURN count(p) as count").single()['count']
        print(f"Total Protocols: {protocol_count}")
        
        # Check Blocks
        block_count = session.run("MATCH (b:Block) RETURN count(b) as count").single()['count']
        print(f"Total Atomic Blocks: {block_count}")
        
        # List blocks for our specific protocol
        print("\nBlocks in 'Phac Do Dieu Tri Mat Ngu Theo Yhct 2023':")
        results = session.run("""
            MATCH (p:Protocol)-[:HAS_BLOCK]->(b) 
            WHERE p.name CONTAINS 'Phac Do Dieu Tri Mat Ngu'
            RETURN b.title as title, b.type as type, b.order as order 
            ORDER BY b.order
        """)
        for record in results:
            print(f" - [{record['order']}] {record['title']} ({record['type']})")
            
        # Check Entities
        ent_count = session.run("MATCH (e:Entity) RETURN count(e) as count").single()['count']
        print(f"\nTotal Medical Entities Linked: {ent_count}")

if __name__ == "__main__":
    try:
        verify()
    finally:
        driver.close()
