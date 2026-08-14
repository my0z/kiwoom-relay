#!/bin/bash
# relay 자동배포 워처 설치 스크립트 - VM에서 한 번만 실행하면 됨.
# 이후로는 GitHub에 커밋만 하면 1분 안에 VM이 스스로 pull + 재시작함.
set -e

REPO_DIR="/home/ubuntu/kiwoom-relay"

echo "== 0. 네트워크 성능 튜닝(BBR) =="
# relay <-> 키움/Worker 간 TCP 처리량/지연 개선. 이미 적용돼 있으면 중복 추가 안 되게 grep으로 확인.
if ! grep -q "^net.ipv4.tcp_congestion_control" /etc/sysctl.conf 2>/dev/null; then
  echo 'net.ipv4.tcp_congestion_control = bbr' | sudo tee -a /etc/sysctl.conf > /dev/null
fi
if ! grep -q "^net.core.default_qdisc" /etc/sysctl.conf 2>/dev/null; then
  echo 'net.core.default_qdisc = fq' | sudo tee -a /etc/sysctl.conf > /dev/null
fi
sudo sysctl -p > /dev/null
echo "현재 혼잡제어 알고리즘: $(sysctl -n net.ipv4.tcp_congestion_control)"

echo "== 1. 워처 스크립트 설치 =="
sudo cp "$REPO_DIR/auto-deploy.sh" /usr/local/bin/kiwoom-auto-deploy.sh
sudo chmod +x /usr/local/bin/kiwoom-auto-deploy.sh

echo "== 2. ubuntu 계정이 비밀번호 없이 relay를 재시작할 수 있게 허용 =="
# 워처가 백그라운드 서비스로 도는데 sudo 비밀번호를 물으면 멈춰버리므로,
# 딱 이 서비스의 restart/is-active 명령에 한해서만 비밀번호를 면제함(전체 sudo 권한 아님).
echo 'ubuntu ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart kiwoom-relay, /usr/bin/systemctl is-active kiwoom-relay' | sudo tee /etc/sudoers.d/kiwoom-auto-deploy > /dev/null
sudo chmod 440 /etc/sudoers.d/kiwoom-auto-deploy
sudo visudo -c -f /etc/sudoers.d/kiwoom-auto-deploy

echo "== 3. systemd 서비스 등록 =="
sudo cp "$REPO_DIR/kiwoom-auto-deploy.service" /etc/systemd/system/kiwoom-auto-deploy.service
sudo systemctl daemon-reload
sudo systemctl enable kiwoom-auto-deploy
sudo systemctl restart kiwoom-auto-deploy

echo "== 4. 상태 확인 =="
sleep 2
sudo systemctl status kiwoom-auto-deploy --no-pager | head -8

echo ""
echo "설치 완료. 앞으로 GitHub에 커밋하면 1분 안에 자동 반영됩니다."
echo "로그 확인:  sudo journalctl -u kiwoom-auto-deploy -f"
