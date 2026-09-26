#!/bin/bash
# === start_rebound.sh ===
# 리바운드 봇(드라이런) 시작
cd ~/upbit || exit 1

if pgrep -f "rebound_bot.py$" > /dev/null; then
    echo "⚠️  이미 실행 중 (PID $(pgrep -f 'rebound_bot.py$'))"
    exit 0
fi

# 가상환경 파이썬 우선 (ccxt/pandas/numpy 설치된 곳)
if [ -x "venv/bin/python" ]; then PY="venv/bin/python"; else PY="python3"; fi

echo "🚀 Starting Rebound bot... ($PY)"
nohup "$PY" rebound_bot.py > rebound_console.log 2>&1 &
sleep 3

if pgrep -f "rebound_bot.py$" > /dev/null; then
    echo "✅ 실행 중 (PID $(pgrep -f 'rebound_bot.py$'))"
    tail -n 5 rebound_bot.log 2>/dev/null
else
    echo "❌ 시작 실패 — 아래 에러 확인"
    tail -n 20 rebound_console.log
fi
