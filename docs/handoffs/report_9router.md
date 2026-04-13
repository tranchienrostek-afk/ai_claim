# Báo cáo trạng thái `9router -> Azure -> agent_claude`

## Kết luận
Stack hiện đã khỏe và dùng được.

Luồng chính đã chạy thông:

`agent_claude / Claude Code -> 9router -> Azure local proxy -> Azure OpenAI`

GLM vẫn giữ vai trò fallback ổn định.

## Kết quả test
| Test | Kết quả |
|---|---|
| `azure-proxy /health` | `ok`, configured, có cả `azure` và `glm` |
| `azure-sonnet` direct @ `128` tokens | pass, `end_turn -> ok` |
| `azure-sonnet` direct @ `512` tokens | pass, `end_turn -> ok` |
| `router-sonnet` combo @ `512` tokens | pass, `end_turn -> ok` |

## Những lỗi đã khóa được
### 1. Windows `EPERM` khi lowdb rename
File:
- `D:\\desktop_folder\\01_claudecodeleak\\9router\\src\\lib\\localDb.js`

Vấn đề:
- lowdb ghi `db.json` theo kiểu atomic rename
- trên Windows có lúc bị AV/indexer giữ file, dẫn tới `EPERM`
- lỗi này làm cascade sau khi Azure request fail

Fix:
- thêm retry tối đa `5` lần cho `EPERM`
- backoff tăng dần `25 -> 400ms`

### 2. Azure stream báo sai `stop_reason` và `usage`
File:
- `D:\\desktop_folder\\01_claudecodeleak\\azure_anthropic_sdk\\azure_anthropic_skd.py`

Vấn đề:
- nhánh Azure stream từng hardcode:
  - `stop_reason = end_turn`
  - `output_tokens = 0`
- làm che mất các trạng thái thật như `max_tokens`, `content_filter`

Fix:
- lấy `finish_reason` thật từ chunk
- map lại qua `map_finish_reason_to_anthropic`
- trả `completion_tokens` thật

### 3. Biến PowerShell bị shadow
File:
- `D:\\desktop_folder\\01_claudecodeleak\\azure_anthropic_sdk\\start_azure_anthropic_server.ps1`

Vấn đề:
- dùng tên `$Args`, đụng biến tự động của PowerShell

Fix:
- đổi sang `$uvicornArgs`
- đồng thời vá luôn nhánh `-Foreground` để dùng đúng `$uvicornArgs`

### 4. Azure proxy chết giữa chừng
Vấn đề:
- local proxy `:8009` từng rơi tiến trình, làm phía `9router` thấy `502 fetch failed`

Fix:
- restart bằng script:
  - `start_azure_anthropic_server.ps1`

## Trạng thái vận hành hiện tại
- `9router` đang là router ngoài cùng
- Azure local proxy đang là lớp Anthropic-compatible cho Azure
- `agent_claude` có thể đi qua:
  - Azure direct alias
  - combo router
  - GLM fallback khi cần

## Ghi chú kỹ thuật
- Fix `localDb.js` là source-level trong `next dev`, nên hot-reload là đủ
- nếu build `9router` production thì cần rebuild lại để mang fix này theo
- GLM path đang ổn; Azure path hiện cũng đã pass test

## File chính liên quan
- `D:\\desktop_folder\\01_claudecodeleak\\9router\\src\\lib\\localDb.js`
- `D:\\desktop_folder\\01_claudecodeleak\\azure_anthropic_sdk\\azure_anthropic_skd.py`
- `D:\\desktop_folder\\01_claudecodeleak\\azure_anthropic_sdk\\start_azure_anthropic_server.ps1`
- `D:\\desktop_folder\\01_claudecodeleak\\azure_anthropic_sdk\\invoke_claude_code_via_9router.ps1`
- `D:\\desktop_folder\\01_claudecodeleak\\azure_anthropic_sdk\\smoke_test_9router_stack.py`

## Lệnh kiểm tra nhanh
```powershell
curl.exe -s http://127.0.0.1:20128/v1/models
curl.exe -s http://127.0.0.1:8009/health
```

```powershell
cd D:\desktop_folder\01_claudecodeleak\azure_anthropic_sdk
python .\smoke_test_9router_stack.py --model azure-sonnet --prompt "Reply with exactly one word: ok"
python .\smoke_test_9router_stack.py --model router-sonnet --prompt "Reply with exactly one word: ok"
```

```powershell
cd D:\desktop_folder\01_claudecodeleak\azure_anthropic_sdk
powershell -ExecutionPolicy Bypass -File .\invoke_claude_code_via_9router.ps1 -Mode combo -Model sonnet -Print -Bare
```

## Kết luận cuối
`agent_claude` qua `9router` hiện đã dùng được ổn định.

Phần việc còn lại, nếu muốn nâng tiếp, sẽ là hardening và tối ưu trải nghiệm:
- quản lý process tự phục hồi
- log/metrics đẹp hơn
- profile mode rõ ràng hơn giữa `azure`, `glm`, `combo`
