$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)))
$cypherPath = Join-Path $PSScriptRoot "agent_topology.cypher"
$containerName = "pathway-neo4j-1"

if (-not (Test-Path -LiteralPath $cypherPath)) {
    throw "Cypher file not found: $cypherPath"
}

docker cp $cypherPath "${containerName}:/tmp/agent_topology.cypher"
docker exec $containerName cypher-shell -u neo4j -p password123 -d neo4j -f /tmp/agent_topology.cypher
docker exec $containerName cypher-shell -u neo4j -p password123 -d neo4j "MATCH (n) WHERE n.topology_id = 'agent_topology_v1' WITH count(n) AS nodes MATCH ()-[r {topology_id:'agent_topology_v1'}]->() RETURN nodes, count(r) AS rels"
