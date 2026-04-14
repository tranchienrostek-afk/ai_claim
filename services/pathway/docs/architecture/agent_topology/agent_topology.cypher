WITH 'agent_topology_v1' AS topology_id
MATCH (n)
WHERE n.topology_id = topology_id
DETACH DELETE n;

WITH 'agent_topology_v1' AS topology_id

MERGE (topology:TopologyMap {topology_id: topology_id, id: 'topology_agent_topology_v1'})
SET topology.name = 'Three Agent Topology',
    topology.caption = 'Agent Topology',
    topology.visual_size = 36,
    topology.status = 'active',
    topology.description = 'Visual map for pathway, agent_orchestrator, agent_claude, and their supporting surfaces.'

MERGE (pathway:Agent {topology_id: topology_id, id: 'agent_pathway'})
SET pathway.name = 'pathway',
    pathway.caption = 'Pathway App',
    pathway.role = 'runtime_application',
    pathway.visual_size = 120,
    pathway.status = 'active',
    pathway.description = 'Live application runtime, FastAPI surface, bridge state, pipeline state, and Neo4j-facing workflow.'

MERGE (orchestrator:Agent {topology_id: topology_id, id: 'agent_orchestrator'})
SET orchestrator.name = 'agent_orchestrator',
    orchestrator.caption = 'AI Messenger',
    orchestrator.role = 'control_plane',
    orchestrator.visual_size = 120,
    orchestrator.status = 'active',
    orchestrator.description = 'Collects evidence from Pathway, packages tasks, dispatches Claude work, and holds approval gates.'

MERGE (claude:Agent {topology_id: topology_id, id: 'agent_claude'})
SET claude.name = 'agent_claude',
    claude.caption = 'Claude Worker',
    claude.role = 'repo_worker',
    claude.visual_size = 120,
    claude.status = 'active',
    claude.description = 'Repo-aware code worker that reads Pathway memory and proposes or applies source changes.'

MERGE (pathway_api:Program {topology_id: topology_id, id: 'program_pathway_api'})
SET pathway_api.name = 'pathway-api-1',
    pathway_api.caption = 'pathway-api-1',
    pathway_api.visual_size = 54,
    pathway_api.status = 'active',
    pathway_api.description = 'Running FastAPI container for Pathway.'

MERGE (neo4j:Program {topology_id: topology_id, id: 'program_neo4j'})
SET neo4j.name = 'pathway-neo4j-1',
    neo4j.caption = 'Neo4j',
    neo4j.visual_size = 54,
    neo4j.status = 'active',
    neo4j.description = 'Graph database used for architecture and ontology views.'

MERGE (claude_cli:Program {topology_id: topology_id, id: 'program_claude_cli'})
SET claude_cli.name = 'claude CLI',
    claude_cli.caption = 'claude -p',
    claude_cli.visual_size = 54,
    claude_cli.status = 'active',
    claude_cli.description = 'Installed Claude Code CLI used as the execution runtime.'

MERGE (fastapi:Program {topology_id: topology_id, id: 'program_fastapi'})
SET fastapi.name = 'FastAPI',
    fastapi.caption = 'FastAPI',
    fastapi.visual_size = 48,
    fastapi.status = 'active',
    fastapi.description = 'HTTP layer used by both Pathway and agent_orchestrator.'

MERGE (docker_exec:Tool {topology_id: topology_id, id: 'tool_docker_exec'})
SET docker_exec.name = 'docker exec',
    docker_exec.caption = 'docker exec',
    docker_exec.visual_size = 48,
    docker_exec.status = 'active',
    docker_exec.description = 'Tool bridge that lets the orchestrator invoke Claude inside the target container.'

MERGE (cypher_shell:Tool {topology_id: topology_id, id: 'tool_cypher_shell'})
SET cypher_shell.name = 'cypher-shell',
    cypher_shell.caption = 'cypher-shell',
    cypher_shell.visual_size = 48,
    cypher_shell.status = 'active',
    cypher_shell.description = 'Tool for loading and iterating on this topology graph.'

MERGE (plugin:Tool {topology_id: topology_id, id: 'tool_pathway_plugin'})
SET plugin.name = 'pathway-intelligence plugin',
    plugin.caption = 'pathway plugin',
    plugin.visual_size = 48,
    plugin.status = 'active',
    plugin.description = 'Project-local Claude skills, hooks, and commands for Pathway reasoning.'

MERGE (mcp_future:MCP {topology_id: topology_id, id: 'mcp_future_bridge'})
SET mcp_future.name = 'MCP bridge',
    mcp_future.caption = 'MCP (planned)',
    mcp_future.visual_size = 48,
    mcp_future.status = 'planned',
    mcp_future.description = 'Future interface for richer tool calls, computer use, or structured context exchange.'

MERGE (api_platform_bootstrap:API {topology_id: topology_id, id: 'api_platform_bootstrap'})
SET api_platform_bootstrap.name = 'GET /api/platform/bootstrap',
    api_platform_bootstrap.caption = 'platform bootstrap',
    api_platform_bootstrap.visual_size = 44,
    api_platform_bootstrap.status = 'active'

MERGE (api_claude_status:API {topology_id: topology_id, id: 'api_claude_status'})
SET api_claude_status.name = 'GET /api/claude/status',
    api_claude_status.caption = 'claude status',
    api_claude_status.visual_size = 44,
    api_claude_status.status = 'active'

MERGE (api_bridge_status:API {topology_id: topology_id, id: 'api_bridge_status'})
SET api_bridge_status.name = 'GET /api/claude/bridge/status',
    api_bridge_status.caption = 'bridge status',
    api_bridge_status.visual_size = 44,
    api_bridge_status.status = 'active'

MERGE (api_pipeline_runs:API {topology_id: topology_id, id: 'api_pipeline_runs'})
SET api_pipeline_runs.name = 'GET /api/pipeline-runs',
    api_pipeline_runs.caption = 'pipeline runs',
    api_pipeline_runs.visual_size = 44,
    api_pipeline_runs.status = 'active'

MERGE (api_task_create:API {topology_id: topology_id, id: 'api_task_create'})
SET api_task_create.name = 'POST /api/tasks/pathway-change',
    api_task_create.caption = 'create task',
    api_task_create.visual_size = 44,
    api_task_create.status = 'active'

MERGE (api_task_execute:API {topology_id: topology_id, id: 'api_task_execute'})
SET api_task_execute.name = 'POST /api/tasks/{task_id}/dispatch-execute',
    api_task_execute.caption = 'dispatch execute',
    api_task_execute.visual_size = 44,
    api_task_execute.status = 'active'

MERGE (api_task_control:API {topology_id: topology_id, id: 'api_task_control'})
SET api_task_control.name = 'POST /api/tasks/{task_id}/control',
    api_task_control.caption = 'task control',
    api_task_control.visual_size = 44,
    api_task_control.status = 'active'

MERGE (fn_evidence:Function {topology_id: topology_id, id: 'fn_collect_pathway_evidence'})
SET fn_evidence.name = 'collect_pathway_evidence()',
    fn_evidence.caption = 'collect evidence',
    fn_evidence.visual_size = 40,
    fn_evidence.status = 'active'

MERGE (fn_envelope:Function {topology_id: topology_id, id: 'fn_build_task_envelope'})
SET fn_envelope.name = 'build_task_envelope()',
    fn_envelope.caption = 'build envelope',
    fn_envelope.visual_size = 40,
    fn_envelope.status = 'active'

MERGE (fn_prompt:Function {topology_id: topology_id, id: 'fn_build_prompt'})
SET fn_prompt.name = 'build_claude_prompt_from_payload()',
    fn_prompt.caption = 'build prompt',
    fn_prompt.visual_size = 40,
    fn_prompt.status = 'active'

MERGE (fn_execute:Function {topology_id: topology_id, id: 'fn_execute_task'})
SET fn_execute.name = 'execute_task()',
    fn_execute.caption = 'execute task',
    fn_execute.visual_size = 40,
    fn_execute.status = 'active'

MERGE (fn_parse:Function {topology_id: topology_id, id: 'fn_parse_response'})
SET fn_parse.name = 'parse_claude_response()',
    fn_parse.caption = 'parse response',
    fn_parse.visual_size = 40,
    fn_parse.status = 'active'

MERGE (fn_bridge:Function {topology_id: topology_id, id: 'fn_pathway_bridge'})
SET fn_bridge.name = 'Pathway Claude Bridge',
    fn_bridge.caption = 'claude bridge',
    fn_bridge.visual_size = 40,
    fn_bridge.status = 'active'

MERGE (fn_decision_gate:Function {topology_id: topology_id, id: 'fn_decision_gate'})
SET fn_decision_gate.name = 'Decision Gate',
    fn_decision_gate.caption = 'decision gate',
    fn_decision_gate.visual_size = 40,
    fn_decision_gate.status = 'active'

MERGE (mem_claude_md:Memory {topology_id: topology_id, id: 'mem_claude_md'})
SET mem_claude_md.name = '/app/CLAUDE.md',
    mem_claude_md.caption = 'CLAUDE.md',
    mem_claude_md.visual_size = 42,
    mem_claude_md.status = 'active'

MERGE (mem_project_map:Memory {topology_id: topology_id, id: 'mem_project_map'})
SET mem_project_map.name = '/app/CLAUDE_PROJECT_MAP.md',
    mem_project_map.caption = 'PROJECT MAP',
    mem_project_map.visual_size = 42,
    mem_project_map.status = 'active'

MERGE (mem_runtime:Memory {topology_id: topology_id, id: 'mem_runtime'})
SET mem_runtime.name = '/app/CLAUDE_RUNTIME_MEMORY.md',
    mem_runtime.caption = 'RUNTIME MEMORY',
    mem_runtime.visual_size = 42,
    mem_runtime.status = 'active'

MERGE (mem_tasks:Memory {topology_id: topology_id, id: 'mem_tasks'})
SET mem_tasks.name = 'data/tasks/*.json',
    mem_tasks.caption = 'task store',
    mem_tasks.visual_size = 42,
    mem_tasks.status = 'active'

MERGE (mem_bridge_logs:Memory {topology_id: topology_id, id: 'mem_bridge_logs'})
SET mem_bridge_logs.name = 'data/claude_bridge/*',
    mem_bridge_logs.caption = 'bridge logs',
    mem_bridge_logs.visual_size = 42,
    mem_bridge_logs.status = 'active'

FOREACH (n IN [pathway, orchestrator, claude, pathway_api, neo4j, claude_cli, fastapi, docker_exec, cypher_shell, plugin, mcp_future, api_platform_bootstrap, api_claude_status, api_bridge_status, api_pipeline_runs, api_task_create, api_task_execute, api_task_control, fn_evidence, fn_envelope, fn_prompt, fn_execute, fn_parse, fn_bridge, fn_decision_gate, mem_claude_md, mem_project_map, mem_runtime, mem_tasks, mem_bridge_logs] |
  MERGE (topology)-[:HAS_NODE {topology_id: topology_id}]->(n)
)

MERGE (pathway)-[:USES {topology_id: topology_id, why: 'runtime process'}]->(pathway_api)
MERGE (pathway)-[:USES {topology_id: topology_id, why: 'graph storage'}]->(neo4j)
MERGE (pathway)-[:USES {topology_id: topology_id, why: 'http runtime'}]->(fastapi)
MERGE (pathway)-[:EXPOSES {topology_id: topology_id}]->(api_platform_bootstrap)
MERGE (pathway)-[:EXPOSES {topology_id: topology_id}]->(api_claude_status)
MERGE (pathway)-[:EXPOSES {topology_id: topology_id}]->(api_bridge_status)
MERGE (pathway)-[:EXPOSES {topology_id: topology_id}]->(api_pipeline_runs)
MERGE (pathway)-[:OWNS {topology_id: topology_id}]->(fn_bridge)
MERGE (pathway)-[:OWNS {topology_id: topology_id}]->(fn_decision_gate)
MERGE (pathway)-[:WRITES {topology_id: topology_id}]->(mem_bridge_logs)
MERGE (pathway)-[:WRITES {topology_id: topology_id}]->(mem_runtime)

MERGE (orchestrator)-[:USES {topology_id: topology_id}]->(fastapi)
MERGE (orchestrator)-[:USES {topology_id: topology_id}]->(docker_exec)
MERGE (orchestrator)-[:USES {topology_id: topology_id}]->(cypher_shell)
MERGE (orchestrator)-[:OWNS {topology_id: topology_id}]->(api_task_create)
MERGE (orchestrator)-[:OWNS {topology_id: topology_id}]->(api_task_execute)
MERGE (orchestrator)-[:OWNS {topology_id: topology_id}]->(api_task_control)
MERGE (orchestrator)-[:OWNS {topology_id: topology_id}]->(fn_evidence)
MERGE (orchestrator)-[:OWNS {topology_id: topology_id}]->(fn_envelope)
MERGE (orchestrator)-[:OWNS {topology_id: topology_id}]->(fn_prompt)
MERGE (orchestrator)-[:OWNS {topology_id: topology_id}]->(fn_execute)
MERGE (orchestrator)-[:OWNS {topology_id: topology_id}]->(fn_parse)
MERGE (orchestrator)-[:STORES {topology_id: topology_id}]->(mem_tasks)
MERGE (orchestrator)-[:COLLECTS_FROM {topology_id: topology_id}]->(api_platform_bootstrap)
MERGE (orchestrator)-[:COLLECTS_FROM {topology_id: topology_id}]->(api_claude_status)
MERGE (orchestrator)-[:COLLECTS_FROM {topology_id: topology_id}]->(api_bridge_status)
MERGE (orchestrator)-[:COLLECTS_FROM {topology_id: topology_id}]->(api_pipeline_runs)
MERGE (orchestrator)-[:DISPATCHES_TO {topology_id: topology_id}]->(claude)
MERGE (orchestrator)-[:PLANS_WITH {topology_id: topology_id, status: 'planned'}]->(mcp_future)

MERGE (claude)-[:RUNS_VIA {topology_id: topology_id}]->(claude_cli)
MERGE (claude)-[:USES {topology_id: topology_id}]->(plugin)
MERGE (claude)-[:READS {topology_id: topology_id}]->(mem_claude_md)
MERGE (claude)-[:READS {topology_id: topology_id}]->(mem_project_map)
MERGE (claude)-[:READS {topology_id: topology_id}]->(mem_runtime)
MERGE (claude)-[:PATCHES_SOURCE_OF {topology_id: topology_id}]->(pathway)
MERGE (claude)-[:RETURNS_REPORT_TO {topology_id: topology_id}]->(orchestrator)

MERGE (docker_exec)-[:INVOKES {topology_id: topology_id}]->(claude_cli)
MERGE (fn_evidence)-[:CALLS {topology_id: topology_id}]->(api_platform_bootstrap)
MERGE (fn_evidence)-[:CALLS {topology_id: topology_id}]->(api_claude_status)
MERGE (fn_evidence)-[:CALLS {topology_id: topology_id}]->(api_bridge_status)
MERGE (fn_evidence)-[:CALLS {topology_id: topology_id}]->(api_pipeline_runs)
MERGE (fn_envelope)-[:WRITES {topology_id: topology_id}]->(mem_tasks)
MERGE (fn_prompt)-[:READS {topology_id: topology_id}]->(mem_claude_md)
MERGE (fn_prompt)-[:READS {topology_id: topology_id}]->(mem_project_map)
MERGE (fn_prompt)-[:READS {topology_id: topology_id}]->(mem_runtime)
MERGE (fn_execute)-[:USES {topology_id: topology_id}]->(docker_exec)
MERGE (fn_execute)-[:CALLS {topology_id: topology_id}]->(claude_cli)
MERGE (fn_parse)-[:SUPPORTS {topology_id: topology_id}]->(fn_execute)
MERGE (fn_bridge)-[:SUPPORTS {topology_id: topology_id}]->(api_claude_status)
MERGE (fn_decision_gate)-[:SUPPORTS {topology_id: topology_id}]->(api_bridge_status)
MERGE (api_task_execute)-[:USES {topology_id: topology_id}]->(fn_execute)
MERGE (api_task_control)-[:USES {topology_id: topology_id}]->(mem_tasks)
MERGE (pathway)-[:EMITS_SIGNAL_TO {topology_id: topology_id}]->(orchestrator)
MERGE (neo4j)-[:VISUALIZES {topology_id: topology_id}]->(topology);
