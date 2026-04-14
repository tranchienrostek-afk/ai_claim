# azure_anthropic_sdk

## Mục tiêu

Thư mục này là một `Anthropic-compatible router` cục bộ.

Ý tưởng là:

```text
Claude Code / client kiểu Anthropic
-> local compatibility server của mình
-> backend thật bên dưới
   - Azure OpenAI
   - GLM
   - về sau có thể thêm provider khác
```

Mục tiêu không chỉ dành cho y tế hay bảo hiểm. Đây là nền tảng tổng quát để về sau `Claude Code` hoặc các app khác không cần gọi trực tiếp LLM của Anthropic.

## Những gì đang có

- [azure_anthropic_skd.py](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/azure_anthropic_skd.py)
  SDK/router lõi:
  - đọc env
  - chọn provider theo `model alias` hoặc `ANTHROPIC_COMPAT_DEFAULT_PROVIDER`
  - route sang `Azure OpenAI` hoặc `GLM`
  - trả response kiểu gần Anthropic

- [anthropic_compat_server.py](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/anthropic_compat_server.py)
  FastAPI server:
  - `GET /health`
  - `POST /v1/messages`

- [start_azure_anthropic_server.ps1](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/start_azure_anthropic_server.ps1)
  Bật local server.

- [stop_azure_anthropic_server.ps1](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/stop_azure_anthropic_server.ps1)
  Tắt local server.

- [invoke_claude_code_via_azure_proxy.ps1](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/invoke_claude_code_via_azure_proxy.ps1)
  Chạy `Claude Code` qua local proxy.
  Giờ đã hỗ trợ:
  - `-Provider azure`
  - `-Provider glm`
  - `-Provider custom`

- [smoke_test_proxy.py](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/smoke_test_proxy.py)
  Smoke test local proxy.

- [.env.example](D:/desktop_folder/01_claudecodeleak/azure_anthropic_sdk/.env.example)
  Mẫu env cho nhiều provider.

## Cấu hình

### Azure

- `AZURE_OPENAI_ENDPOINT`
- `AZURE_OPENAI_API_KEY`
- `AZURE_OPENAI_API_VERSION`
- `AZURE_DEPLOYMENT_NAME`
- `AZURE_TEMPERATURE`
- `AZURE_REQUEST_TIMEOUT_SECONDS`

### GLM

- `GLM_BASE_URL`
- `GLM_API_KEY`
- `GLM_MODEL_NAME`
- `GLM_MODEL_VERSION`
- `GLM_REQUEST_TIMEOUT_SECONDS`

### Routing

- `ANTHROPIC_COMPAT_DEFAULT_PROVIDER`
  - `azure`
  - `glm`

### Local server

- `PROXY_PORT`

## Cách chọn provider

SDK chọn provider theo thứ tự:

1. nếu `model alias` bắt đầu bằng `glm` thì dùng GLM
2. nếu `model alias` bắt đầu bằng `azure` thì dùng Azure
3. nếu không có prefix thì dùng `ANTHROPIC_COMPAT_DEFAULT_PROVIDER`
4. nếu default không hợp lệ thì fallback sang provider nào đang được cấu hình

Ví dụ:

- `azure-sonnet` -> Azure
- `azure-opus` -> Azure
- `glm-sonnet` -> GLM
- `glm-opus` -> GLM
- `glm-5-turbo` -> GLM

## Trạng thái hiện tại

Đã verify local canary trên máy này ngày `2026-04-10`:

### Azure

- local server lên được
- `GET /health` OK
- `POST /v1/messages` OK
- `Claude Code` release binary đi qua local proxy rồi vào Azure thành công

### GLM

- SDK đã được nối sẵn để route sang GLM qua `https://api.z.ai/api/anthropic`
- local proxy đã hỗ trợ chọn backend GLM bằng alias `glm-*` hoặc `-Provider glm`
- local run hôm nay đã route đúng tới GLM, nhưng upstream hiện trả `429 Usage limit reached`, nên đây là trạng thái quota chứ không phải lỗi routing của local proxy

## Lệnh chạy

### 1. Bật local server

```powershell
cd D:\desktop_folder\01_claudecodeleak\azure_anthropic_sdk
powershell -ExecutionPolicy Bypass -File .\start_azure_anthropic_server.ps1
```

### 2. Kiểm tra health

```powershell
curl.exe -s http://127.0.0.1:8009/health
```

### 3. Smoke test local proxy

```powershell
cd D:\desktop_folder\01_claudecodeleak\azure_anthropic_sdk
python .\smoke_test_proxy.py
```

### 4. Chạy Claude Code qua Azure

```powershell
cd D:\desktop_folder\01_claudecodeleak\azure_anthropic_sdk
powershell -ExecutionPolicy Bypass -File .\invoke_claude_code_via_azure_proxy.ps1 `
  -Provider azure `
  -Model sonnet `
  -Prompt "Tra loi bang mot cau tieng Viet that ngan." `
  -Print `
  -Bare
```

### 5. Chạy Claude Code qua GLM

```powershell
cd D:\desktop_folder\01_claudecodeleak\azure_anthropic_sdk
powershell -ExecutionPolicy Bypass -File .\invoke_claude_code_via_azure_proxy.ps1 `
  -Provider glm `
  -Model sonnet `
  -Prompt "Tra loi bang mot cau tieng Viet that ngan." `
  -Print `
  -Bare
```

## Cách hoạt động với Claude Code

`Claude Code` vẫn nghĩ rằng nó đang gọi một endpoint kiểu Anthropic:

- `ANTHROPIC_BASE_URL=http://127.0.0.1:8009`
- `ANTHROPIC_API_KEY=local-anthropic-proxy`
- `ANTHROPIC_DEFAULT_SONNET_MODEL=azure-sonnet` hoặc `glm-sonnet`
- `ANTHROPIC_DEFAULT_OPUS_MODEL=azure-opus` hoặc `glm-opus`

Nhưng local proxy của mình sẽ là lớp quyết định backend thật sự.

## Những gì đã hỗ trợ

- `messages`
- `system`
- `max_tokens`
- `tools`
- `tool_choice` cơ bản
- non-stream
- SSE stream cơ bản
- route Azure
- route GLM
- fallback cho vài khác biệt của Azure:
  - `max_tokens -> max_completion_tokens`
  - bỏ `temperature` nếu model không hỗ trợ custom temperature

## Những gì chưa thể coi là full Claude Code parity

- chưa full parity cho `thinking` và `signature`
- chưa full parity cho streamed `tool_use`
- chưa mô phỏng toàn bộ session semantics
- chưa battle-test toàn bộ command của `Claude Code`

Nói ngắn gọn:

- dùng được cho app Python của mình
- dùng được để canary `Claude Code` qua local proxy
- đã chạy được với Azure
- đã hỗ trợ GLM trong cùng kiến trúc
- chưa nên coi là drop-in 100% cho toàn bộ hành vi của `Claude Code`

## Hướng phát triển tiếp

### P0

- giữ vững non-stream compatibility
- giữ canary ổn cho Azure và GLM
- ghi log request/response tốt hơn

### P1

- làm tốt hơn parity của tool-use
- làm tốt hơn streaming parity
- thêm test fixtures cho nhiều dạng content block

### P2

- thêm provider khác ngoài Azure và GLM
- session replay
- provider policy
- fallback routing nâng cao

## Kết luận

Thư mục này giờ không còn là “Azure-only” nữa.

Nó là local router kiểu Anthropic-compatible để:

- `Claude Code` hoặc client kiểu Anthropic chạy qua local proxy của mình
- backend bên dưới có thể là `Azure`, `GLM`, rồi về sau thêm model khác
- giảm phụ thuộc vào việc gọi trực tiếp hạ tầng LLM của Anthropic
