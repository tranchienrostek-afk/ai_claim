# 9Router Stack

Muc tieu:

- `Claude Code` noi truc tiep vao `9router`
- `9router` dung combo/fallback va provider management
- Azure di qua local proxy trong thu muc `azure_anthropic_sdk`
- GLM di native qua `9router`

Flow:

```text
Claude Code
-> 9router /v1/messages
-> Azure Local Proxy (anthropic-compatible node)
-> Azure OpenAI

fallback:

Claude Code
-> 9router /v1/messages
-> GLM native provider
```

Files:

- [bootstrap_9router_stack.py](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/bootstrap_9router_stack.py)
- [start_9router_stack.ps1](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/start_9router_stack.ps1)
- [stop_9router_stack.ps1](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/stop_9router_stack.ps1)
- [invoke_claude_code_via_9router.ps1](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/invoke_claude_code_via_9router.ps1)
- [smoke_test_9router_stack.py](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/smoke_test_9router_stack.py)

Bootstrap artifacts:

- `9router/.env.local`
- `azure_anthropic_sdk/runtime/9router_data/db.json`
- `azure_anthropic_sdk/runtime/9router_stack.json`

Commands:

```powershell
cd D:\desktop_folder\01_claudecodeleak\azure_anthropic_sdk
python .\bootstrap_9router_stack.py
powershell -ExecutionPolicy Bypass -File .\start_9router_stack.ps1 -SkipNpmInstall
python .\smoke_test_9router_stack.py --model router-sonnet
powershell -ExecutionPolicy Bypass -File .\invoke_claude_code_via_9router.ps1 -Mode combo -Model sonnet -Print -Bare
powershell -ExecutionPolicy Bypass -File .\stop_9router_stack.ps1
```

Notes:

- `router-sonnet` va `router-opus` la combo mac dinh.
- `azure-sonnet` va `azure-opus` di truc tiep vao Azure local proxy.
- `glm-sonnet` va `glm-opus` di truc tiep vao GLM native.
- `agent_claude` / `claude` canary da chay thanh cong qua `9router -> Azure`.
