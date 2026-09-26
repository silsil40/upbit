#!/bin/bash
# === stats_rebound.sh ===
# 리바운드 봇 상태 + 드라이런 성적표
cd ~/upbit || exit 1
if [ -x "venv/bin/python" ]; then PY="venv/bin/python"; else PY="python3"; fi

echo "================ 프로세스 ================"
PID=$(pgrep -f "rebound_bot.py$")
if [ -n "$PID" ]; then
    ps -o pid,etime,rss,pcpu,cmd -p "$PID" | awk 'NR==1{print "PID  가동시간  메모리(KB)  CPU%  명령"; next}{print}'
else
    echo "❌ 실행 중 아님  (./start_rebound.sh 로 시작)"
fi

echo
echo "================ 서버 메모리 ================"
free -m | awk 'NR<=2 || /Swap/'

echo
echo "================ 최근 로그 ================"
tail -n 12 rebound_bot.log 2>/dev/null || echo "(로그 없음)"

echo
"$PY" rebound_bot.py --report
