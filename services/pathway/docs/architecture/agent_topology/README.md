# Agent Topology In Neo4j

This package creates a dedicated architecture subgraph for the three-agent system:

- `pathway`
- `agent_orchestrator`
- `agent_claude`

The intent is to keep one visual map for:

- the three big agent nodes
- the smaller nodes they use to communicate
- the APIs, functions, programs, MCP, tools, and memory surfaces involved

## Files

- `agent_topology.cypher`
  - wipes and recreates the dedicated subgraph `agent_topology_v1`
- `README.md`
  - usage, queries, and Browser style hints

## Safe Scope

This graph is isolated with:

- root node label: `TopologyMap`
- node property: `topology_id = 'agent_topology_v1'`

That means it does not interfere with the medical ontology graph already in Neo4j.

## Load Into Neo4j

Recommended:

```powershell
cd D:\desktop_folder\01_claudecodeleak
.\pathway\notebooklm\docs\architecture\agent_topology\load_agent_topology.ps1
```

Manual method:

```powershell
docker cp pathway\notebooklm\docs\architecture\agent_topology\agent_topology.cypher pathway-neo4j-1:/tmp/agent_topology.cypher
docker exec pathway-neo4j-1 cypher-shell -u neo4j -p password123 -d neo4j -f /tmp/agent_topology.cypher
```

## Open In Neo4j Browser

Browser URL:

```text
http://localhost:7475/browser/
```

Main query:

```cypher
MATCH (t:TopologyMap {topology_id: 'agent_topology_v1'})
MATCH (t)-[:HAS_NODE]->(n)
OPTIONAL MATCH (n)-[r]->(m)
WHERE m.topology_id = 'agent_topology_v1'
RETURN t, n, r, m
```

## Helpful Focus Queries

Only the 3 main agents:

```cypher
MATCH (n:Agent {topology_id: 'agent_topology_v1'})
RETURN n
```

Everything around `agent_orchestrator`:

```cypher
MATCH (o {topology_id: 'agent_topology_v1', id: 'agent_orchestrator'})
OPTIONAL MATCH (o)-[r]-(n)
WHERE n.topology_id = 'agent_topology_v1'
RETURN o, r, n
```

Only API and function surfaces:

```cypher
MATCH (n {topology_id: 'agent_topology_v1'})
WHERE n:API OR n:Function
OPTIONAL MATCH (n)-[r]-(m)
WHERE m.topology_id = 'agent_topology_v1'
RETURN n, r, m
```

## Why Colors Looked The Same Before

Previously, every node shared common labels like `AgentTopology` and `ArchitectureNode`.
Neo4j Browser often lets a shared label dominate the default style, so many nodes looked the same.

This version fixes that in two ways:

- each visual node now has a dominant type label like `Agent`, `API`, `Function`, `Program`, `Tool`, `Memory`, or `MCP`
- a separate root node `TopologyMap` holds the grouping

So even without custom style, Browser should distinguish node families much better.

## Neo4j Browser Style Hint

If you want stronger visuals, load the provided style file:

```text
:style
```

Then paste the contents of:

```text
pathway/notebooklm/docs/architecture/agent_topology/agent_topology_browser.grass
```

Or use this starter style:

```text
node.Agent { caption: '{caption}'; color: #f97316; border-color: #9a3412; }
node.API { caption: '{caption}'; color: #3b82f6; border-color: #1d4ed8; }
node.Function { caption: '{caption}'; color: #22c55e; border-color: #15803d; }
node.Program { caption: '{caption}'; color: #a855f7; border-color: #6b21a8; }
node.Tool { caption: '{caption}'; color: #f59e0b; border-color: #b45309; }
node.Memory { caption: '{caption}'; color: #78716c; border-color: #44403c; }
node.MCP { caption: '{caption}'; color: #06b6d4; border-color: #0e7490; }
```

## How To Extend Later

Good next steps:

- add per-agent source-file nodes
- add edges for `reads`, `writes`, `dispatches`, `approves`, `deploys`
- add `status = planned / active / deprecated`
- add `team_owner`
- add `priority`
- add `risk_level`

This graph is meant to become the living architecture map for the three-agent system, not just a one-off demo.
