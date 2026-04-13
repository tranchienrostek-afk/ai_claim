# Start AI Claim Server
$env:PYTHONPATH = "src"
uvicorn ai_claim.main:app --host 127.0.0.1 --port 9780 --reload
