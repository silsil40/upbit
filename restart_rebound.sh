#!/bin/bash
# === restart_rebound.sh ===
# 리바운드 봇 재시작 (GitHub 최신화 → 재실행)
echo "🔁 Restarting Rebound bot..."
cd ~/upbit || exit 1

# 봇 정지
./stop_rebound.sh

# 최신 코드 갱신
echo "📥 Updating from GitHub..."
git pull

# 가상환경 + 의존성 (ccxt/pandas/numpy 가 requirements.txt 에 있어야 함)
if [ ! -d "venv" ]; then
    echo "⚙️  가상환경이 없습니다. 새로 생성합니다..."
    python3 -m venv venv
fi
source venv/bin/activate
pip install -r requirements.txt --quiet

# 봇 재실행
./start_rebound.sh
echo "✅ Restart complete."
