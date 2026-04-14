import os
from neo4j import GraphDatabase

def cleanup_zola_to_zona():
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7688")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    with driver.session() as session:
        # 1. Find all properties of all nodes that contain "Zola" (case insensitive)
        print("Searching for 'Zola' occurrences in Neo4j...")
        
        # We'll use a two-step approach: 
        # a) Update properties of nodes
        # b) Update metadata/tags if any
        
        # Cypher for case-insensitive replace in all string properties
        # This is a bit complex in pure Cypher for ALL properties, so we'll target known fields 
        # but also any string field.
        
        query = """
        MATCH (n)
        WHERE any(prop IN keys(n) WHERE apoc.meta.type(n[prop]) = 'STRING' AND n[prop] =~ '(?i).*zola.*')
        RETURN id(n) as id, labels(n) as labels, keys(n) as props
        """
        
        # Note: If apoc is not available, we use a simpler filter
        query_no_apoc = """
        MATCH (n)
        WHERE any(prop IN keys(n) WHERE toString(n[prop]) =~ '(?i).*zola.*')
        RETURN id(n) as id, labels(n) as labels, keys(n) as props
        """
        
        try:
            nodes_to_fix = list(session.run(query_no_apoc))
            print(f"Found {len(nodes_to_fix)} nodes containing 'Zola'.")
            
            for record in nodes_to_fix:
                node_id = record["id"]
                props = record["props"]
                
                for prop in props:
                    # Update each property that contains "zola"
                    update_query = f"""
                    MATCH (n) WHERE id(n) = $id
                    AND toString(n["{prop}"]) =~ '(?i).*zola.*'
                    SET n["{prop}"] = apoc.text.replace(toString(n["{prop}"]), '(?i)zola', 'Zona')
                    """
                    
                    # Fallback if apoc.text.replace is not available
                    update_query_no_apoc = f"""
                    MATCH (n) WHERE id(n) = $id
                    AND toString(n["{prop}"]) =~ '(?i).*zola.*'
                    WITH n, n["{prop}"] as old_val
                    SET n["{prop}"] = replace(old_val, 'Zola', 'Zona')
                    """
                    
                    # Also try lower case and upper case replace
                    update_query_layered = f"""
                    MATCH (n) WHERE id(n) = $id
                    SET n["{prop}"] = replace(replace(replace(toString(n["{prop}"]), 'Zola', 'Zona'), 'zola', 'zona'), 'ZOLA', 'ZONA')
                    """
                    
                    session.run(update_query_layered, id=node_id)
                
                print(f"  Fixed node {node_id} ({record['labels']})")
                
            print("\nCleanup complete.")
            
        except Exception as e:
            print(f"Error during cleanup: {e}")
        
    driver.close()

if __name__ == "__main__":
    cleanup_zola_to_zona()
