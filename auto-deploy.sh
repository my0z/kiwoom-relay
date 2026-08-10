#!/bin/bash
# relay 자동 배포 워처
# - 1분마다 GitHub 원격(origin/main)에 새 커밋이 있는지 확인
# - 새 커밋이 있으면 pull -> 문법검사(node -c) -> 통과할 때만 relay 재시작
# - 문법 오류가 있는 커밋이 올라와도 relay를 죽이지 않고 이전 버전 그대로 유지함(안전장치)
#
# 설치:
#   sudo cp auto-deploy.sh /usr/local/bin/kiwoom-auto-deploy.sh
#   sudo chmod +x /usr/local/bin/kiwoom-auto-deploy.sh
#   (systemd 서비스 등록은 kiwoom-auto-deploy.service 참고)

REPO_DIR="/home/ubuntu/kiwoom-relay"
SERVICE="kiwoom-relay"
INTERVAL=60   # 확인 주기(초)

cd "$REPO_DIR" || { echo "저장소 경로를 찾을 수 없음: $REPO_DIR"; exit 1; }

echo "[auto-deploy] 감시 시작 (저장소=$REPO_DIR, 주기=${INTERVAL}초)"

while true; do
  # 원격 정보만 갱신 (작업트리는 아직 안 건드림)
  if git fetch origin main --quiet 2>/dev/null; then
    LOCAL=$(git rev-parse HEAD 2>/dev/null)
    REMOTE=$(git rev-parse origin/main 2>/dev/null)

    if [ -n "$LOCAL" ] && [ -n "$REMOTE" ] && [ "$LOCAL" != "$REMOTE" ]; then
      echo "[auto-deploy] 새 커밋 감지: ${LOCAL:0:7} -> ${REMOTE:0:7}"

      # 문제가 생기면 되돌아올 수 있게 현재 커밋을 기억해둠
      PREV="$LOCAL"

      if git pull --ff-only origin main --quiet 2>/dev/null; then
        # 문법 검사를 통과할 때만 재시작 - 깨진 코드로 relay가 죽는 것을 막는 안전장치
        if node -c relay.js 2>/dev/null; then
          echo "[auto-deploy] 문법 검사 통과 - relay 재시작"
          if sudo systemctl restart "$SERVICE"; then
            sleep 3
            if systemctl is-active --quiet "$SERVICE"; then
              echo "[auto-deploy] 재시작 완료 (현재 ${REMOTE:0:7})"
            else
              echo "[auto-deploy] 재시작 후 서비스가 죽어있음 - 이전 커밋으로 되돌림"
              git reset --hard "$PREV" --quiet
              sudo systemctl restart "$SERVICE"
            fi
          else
            echo "[auto-deploy] 재시작 명령 실패"
          fi
        else
          echo "[auto-deploy] 문법 오류 - 이전 커밋으로 되돌리고 재시작 안 함"
          git reset --hard "$PREV" --quiet
        fi
      else
        echo "[auto-deploy] pull 실패(충돌 등) - 수동 확인 필요"
      fi
    fi
  fi

  sleep "$INTERVAL"
done
