# Run from the project folder in PowerShell:  .\start_all.ps1
$p = (Get-Location).Path
function Run($cmd) { Start-Process powershell -ArgumentList "-NoExit","-Command","cd '$p'; . '$p\venv\Scripts\Activate.ps1'; $cmd" }
docker compose up -d; Write-Host "Waiting 45s for Kafka + HDFS..."; Start-Sleep 45
. "$p\venv\Scripts\Activate.ps1"; python prepare_data.py
Run "python consumer.py c1"; Run "python consumer.py c2"
Run "python forecast_service.py --loop 45 --auto"; Run "streamlit run dashboard.py"
Start-Sleep 8; Run "python producer.py 40"
