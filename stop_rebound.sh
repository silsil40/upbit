#!/bin/bash
# === stop_rebound.sh ===
# 리바운드 봇 중지 (상태는 파일에 저장돼 있어 다시 켜면 이어서 진행)
cd ~/upbit || exit 1

if ! pgrep -f "rebound_bot.py$" > /dev/null; then
    echo "ℹ️  실행 중 아님"
    exit 0
fi

echo "🛑 Stopping Rebound bot... (PID $(pgrep -f 'rebound_bot.py$'))"
pkill -f "rebound_bot.py$"

# 최대 10초 종료 대기
for i in $(seq 1 10); do
    if ! pgrep -f "rebound_bot.py$" > /dev/null; then
        echo "✅ 중지됨"
        exit 0
    fi
    sleep 1
done

echo "⚠️  10초 내 종료 안 됨 → 강제 종료"
pkill -9 -f "rebound_bot.py$"
sleep 1
pgrep -f "rebound_bot.py$" > /dev/null && echo "❌ 종료 실패" || echo "✅ 강제 중지됨"
