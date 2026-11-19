#!/usr/bin/env bash
# Run on an AL2023 instance after validating the versioned artifact SHA256.
set -euo pipefail
umask 077

if [[ $# != 12 ]]; then
  echo "usage: bootstrap.sh region role id secret-id secret-prefix bucket key version sha256 metadata-volume memory-mib bundle" >&2
  exit 2
fi
region=$1 role=$2 node=$3 node_secret=$4 prefix=$5 metadata=${10} memory_mib=${11} bundle=${12}

[[ $role == controller || $role == worker ]]
printf '%s  %s\n' "${9}" "$bundle" | sha256sum --check --status
work=$(mktemp -d /run/nabedra-cache-install.XXXXXX)
trap 'rm -f "$work/nabedra-db" "$work/reconcile.py" "$work/observations.py" "$work/clockbound" "$work/secret"; rmdir "$work"' EXIT
for name in nabedra-db reconcile.py observations.py clockbound; do
  tar -xOzf "$bundle" "$name" >"$work/$name"
done
install -m 0755 "$work/nabedra-db" /usr/local/bin/nabedra-db
[[ $(/usr/local/bin/nabedra-db --version) == *" cache" ]]
install -d -m 0755 /usr/local/lib/nabedra-cache
install -m 0755 "$work/reconcile.py" /usr/local/lib/nabedra-cache/reconcile.py
install -m 0755 "$work/observations.py" /usr/local/lib/nabedra-cache/observations.py

if ! getent passwd nabedra-cache >/dev/null; then
  useradd --system --shell /sbin/nologin nabedra-cache
fi
install -d -o root -g nabedra-cache -m 0750 /etc/nabedra-cache
install -d -o root -g nabedra-cache -m 0750 /etc/nabedra-cache/node

install_secret() {
  local secret_id=$1 destination=$2 role=$3
  for attempt in $(seq 1 18); do
    aws --region "$region" secretsmanager get-secret-value       --secret-id "$secret_id" --query SecretString --output text >"$work/secret" && break
    sleep 5
  done
  python3 - "$destination" "$role" "$work/secret" <<'PY'
import json
import os
from pathlib import Path
import sys

destination, role, source = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
files = json.loads(source.read_text(encoding="utf-8"))["files"]
allowed = {"node.toml", "node.pem", "node.key", "ca.pem"}
if role == "operator":
    allowed |= {f"member-{node}.toml" for node in range(1, 13)}
assert set(files) == allowed, "unexpected bootstrap secret files"
for name, value in files.items():
    path = destination / name
    temporary = destination / f".{name}.new"
    temporary.write_text(value, encoding="utf-8")
    os.chmod(temporary, 0o600 if role == "operator" else 0o640)
    if role != "operator":
        import grp
        os.chown(temporary, 0, grp.getgrnam("nabedra-cache").gr_gid)
    temporary.replace(path)
PY
  rm -f "$work/secret"
}

install_secret "$node_secret" /etc/nabedra-cache/node "$role"
if [[ "$role" == controller && "$node" == 1 ]]; then
  install -d -o root -g root -m 0700 /etc/nabedra-cache/operator
  install_secret "$prefix/operator" /etc/nabedra-cache/operator operator
fi

if [[ $role == controller ]]; then
  [[ $metadata == vol-* ]]
  disk="/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_${metadata//-/}"
  for attempt in $(seq 1 180); do
    [[ -b "$disk" ]] && break
    sleep 2
  done
  [[ -b "$disk" ]]
  install -d -m 0755 /var/lib/nabedra-cache
  disk_format=$(blkid -p -s TYPE -o value "$disk" || true)
  if [[ -z $disk_format ]]; then
    [[ -z $(wipefs -n -o TYPE --noheadings "$disk") ]]
    mkfs.ext4 -q "$disk"
    disk_format=ext4
  fi
  [[ $disk_format == ext4 ]]
  uuid=$(blkid -p -s UUID -o value "$disk")
  [[ -n $uuid ]]
  if ! grep -Fq "UUID=$uuid /var/lib/nabedra-cache " /etc/fstab; then
    printf 'UUID=%s /var/lib/nabedra-cache ext4 defaults,nofail,x-systemd.device-timeout=3min 0 2\n' "$uuid" >>/etc/fstab
  fi
  mountpoint -q /var/lib/nabedra-cache || mount /var/lib/nabedra-cache
  [[ $(findmnt -rn -M /var/lib/nabedra-cache -o UUID) == "$uuid" ]]
  chown nabedra-cache:nabedra-cache /var/lib/nabedra-cache
  chmod 0700 /var/lib/nabedra-cache
else
  swapoff --all
  install -d -m 0755 /etc/systemd/system/nabedra-cache.service.d
  cat >/etc/systemd/system/nabedra-cache.service.d/swap.conf <<'UNIT'
[Service]
MemorySwapMax=0
UNIT
fi

install -m 0755 "$work/clockbound" /usr/local/bin/clockbound
clock_service=clockbound.service
if ! grep -Eq '^server[[:space:]]+169\.254\.169\.123([[:space:]]|$)' /etc/chrony.conf; then
  printf '\nserver 169.254.169.123 prefer iburst\n' >>/etc/chrony.conf
fi
sed -i '/^[[:space:]]*maxclockerror[[:space:]]/d' /etc/chrony.conf
printf 'maxclockerror 50\n' >>/etc/chrony.conf
cat >/etc/systemd/system/clockbound.service <<'UNIT'
[Unit]
Description=ClockBound AWS bounded-time daemon
After=chronyd.service
Requires=chronyd.service

[Service]
User=chrony
Group=chrony
RuntimeDirectory=clockbound
RuntimeDirectoryMode=0755
RuntimeDirectoryPreserve=yes
WorkingDirectory=/run/clockbound
ExecStartPre=+/bin/chmod a+r /dev/vmclock0
ExecStart=/usr/local/bin/clockbound --max-drift-rate 50
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now chronyd.service
systemctl reload-or-restart chronyd.service
systemctl enable --now "$clock_service"

configured=$(awk '$1=="MemTotal:" {print int($2/1024)}' /proc/meminfo)
(( memory_mib >= 256 && memory_mib + 256 <= configured ))
if [[ $role == worker ]]; then
  # The generated worker config states its charged budget in bytes. Reserve
  # extra RSS headroom before installing the hard cgroup limit.
  cache_budget=$(awk '/^\[client\.limits\]$/ { section=1; next }
    /^\[/ { section=0 }
    section && $1=="memory_bytes" && $2=="=" {print $3; exit}' /etc/nabedra-cache/node/node.toml)
  [[ $cache_budget =~ ^[0-9]+$ ]]
  (( cache_budget + 128 * 1024 * 1024 <= memory_mib * 1024 * 1024 ))
fi
mount_condition=""
if [[ $role == controller ]]; then
  mount_condition="RequiresMountsFor=/var/lib/nabedra-cache"
fi
cat >/etc/systemd/system/nabedra-cache.service <<UNIT
[Unit]
Description=Nabedra Cache $role
After=network-online.target $clock_service
Wants=network-online.target
Requires=$clock_service
$mount_condition

[Service]
ExecStart=/usr/local/bin/nabedra-db cache $role --config /etc/nabedra-cache/node/node.toml
User=nabedra-cache
Group=nabedra-cache
Restart=on-failure
RestartSec=1
TimeoutStopSec=15
LimitNOFILE=16384
TasksMax=128
RuntimeDirectory=nabedra-cache
RuntimeDirectoryMode=0750
MemoryMax=${memory_mib}M
CPUAccounting=yes
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=-/var/lib/nabedra-cache

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now nabedra-cache.service
