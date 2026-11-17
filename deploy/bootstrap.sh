#!/usr/bin/env bash
# bootstrap.sh — install the GPUB CLI (and optionally the HOST AGENT as a systemd --user service) on THIS machine.
#
#   deploy/bootstrap.sh --config <config.json> [--agent <resource-id>] [--nvidia-smi PATH]
#
# <config.json> is {"url": "http://<coordinator private IP>:8766", "machine": "<this machine>", "token": "<token>"};
# the token comes from `coordinator.py register <machine> --ip <this machine's private IP>` on the coordinator host.
# Transfer it over a private channel. A token is useless from another host: the coordinator also requires the request
# to arrive from the IP registered for that machine.
#
# Non-root. No OS packages. No pip: the CLI and the agent are stdlib-only (Python 3.11+).
# The agent is COPIED to ~/.local/lib/gpub, so it keeps running if this checkout goes away.
set -euo pipefail
CONFIG=""; AGENT_RES=""; SMI=""
while [ $# -gt 0 ]; do
  case "$1" in
    --config) CONFIG="${2:?path to config.json}"; shift 2;;
    --agent) AGENT_RES="${2:?resource id}"; shift 2;;
    --nvidia-smi) SMI="${2:?path}"; shift 2;;
    *) echo "unknown arg $1"; exit 2;;
  esac
done
[ -n "$CONFIG" ] && [ -f "$CONFIG" ] || { echo "usage: $0 --config <config.json> [--agent <resource-id>] [--nvidia-smi PATH]"; exit 2; }
REPO="$(cd "$(dirname "$0")/.." && pwd)"
PY="$(command -v python3)"
"$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' || { echo "python3 >= 3.11 required"; exit 2; }

mkdir -p "$HOME/.config/gpub" "$HOME/.local/bin" "$HOME/.local/lib/gpub" "$HOME/.local/state/gpub"
chmod 700 "$HOME/.config/gpub"
if [ -f "$HOME/.config/gpub/config.json" ] && ! cmp -s "$CONFIG" "$HOME/.config/gpub/config.json"; then
  cp "$HOME/.config/gpub/config.json" "$HOME/.config/gpub/config.json.bak.$(date +%Y%m%d%H%M%S)"
  echo "existing config backed up"
fi
install -m 600 "$CONFIG" "$HOME/.config/gpub/config.json"

install -m 755 "$REPO/gpub/cli.py" "$HOME/.local/lib/gpub/cli.py"
cat > "$HOME/.local/bin/gpub" <<WRAP
#!/usr/bin/env bash
exec "$PY" "$HOME/.local/lib/gpub/cli.py" "\$@"
WRAP
chmod 755 "$HOME/.local/bin/gpub"
case ":$PATH:" in *":$HOME/.local/bin:"*) ;; *) echo "NOTE: add \$HOME/.local/bin to PATH";; esac
"$HOME/.local/bin/gpub" time
"$HOME/.local/bin/gpub" status | head -12

if [ -n "$AGENT_RES" ]; then
  install -m 755 "$REPO/gpub/agent.py" "$HOME/.local/lib/gpub/agent.py"
  if [ -z "$SMI" ]; then
    SMI="$(command -v nvidia-smi || true)"
    [ -n "$SMI" ] || { [ -x /usr/lib/wsl/lib/nvidia-smi ] && SMI=/usr/lib/wsl/lib/nvidia-smi; } || true
  fi
  [ -n "$SMI" ] || echo "WARNING: nvidia-smi not found; the agent will report gpu.error (bookings and jobs still work)"
  command -v systemctl >/dev/null || { echo "no systemd here: run $HOME/.local/lib/gpub/agent.py --resource $AGENT_RES under your service manager"; exit 0; }
  mkdir -p "$HOME/.config/systemd/user"
  UNIT="$HOME/.config/systemd/user/gpub-agent.service"
  [ -f "$UNIT" ] && cp "$UNIT" "$UNIT.bak.$(date +%Y%m%d%H%M%S)"
  sed -e "s|@PYTHON@|$PY|g" -e "s|@AGENT@|$HOME/.local/lib/gpub/agent.py|g" -e "s|@RESOURCE@|$AGENT_RES|g" \
      -e "s|@SMI_ARGS@|${SMI:+--nvidia-smi $SMI}|g" -e "s|@STATE@|$HOME/.local/state/gpub|g" \
      "$REPO/deploy/gpub-agent.service" | grep -v '^#' > "$UNIT"
  systemctl --user daemon-reload
  systemctl --user enable --now gpub-agent.service
  sleep 3
  systemctl --user --no-pager status gpub-agent.service | head -8 || true
  tail -5 "$HOME/.local/state/gpub/agent.log" || true
  if loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q 'Linger=yes'; then
    echo "linger: yes (agent survives logout)"
  else
    echo "linger: not enabled. Run: sudo loginctl enable-linger $USER   (until then the agent lives only while you are logged in)"
  fi
fi
echo "bootstrap done for $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["machine"])' "$CONFIG")${AGENT_RES:+ (agent for $AGENT_RES)}"
