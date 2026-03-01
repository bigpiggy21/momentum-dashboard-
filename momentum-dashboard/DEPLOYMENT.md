# Deployment Guide — Momentum Dashboard on Hetzner VPS

## Server Details
- **Provider**: Hetzner (4 vCPU, 8GB RAM, Ubuntu 24.04)
- **Public IP**: 89.167.110.85
- **Tailscale IP**: 100.97.33.3
- **Access URL**: http://100.97.33.3:8080 (Tailscale network only)
- **SSH**: `ssh root@89.167.110.85`
- **GitHub repo**: https://github.com/bigpiggy21/momentum-dashboard- (private)

## Initial Setup (already done)

### 1. VPS provisioning
- Created Hetzner CX server, Ubuntu 24.04
- SSH'd in with root password from Hetzner email, changed password on first login

### 2. Installed dependencies
```bash
apt update && apt install -y python3 python3-pip git unzip
pip3 install pandas numpy requests scipy --break-system-packages
```

### 3. Installed Tailscale
```bash
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up
# Authorised via browser link in Tailscale admin panel
```

### 4. Cloned repo
```bash
git clone https://github.com/bigpiggy21/momentum-dashboard-.git /root/momentum-dashboard
```

### 5. Transferred data files (from Windows PowerShell, NOT the VPS)
```powershell
# Cache (~900MB) — zipped first for speed
Compress-Archive -Path "C:\Users\david\Downloads\momentum-dashboard\momentum-dashboard\cache" -DestinationPath "C:\Users\david\Downloads\cache.zip"
scp "C:\Users\david\Downloads\cache.zip" root@89.167.110.85:/root/momentum-dashboard/momentum-dashboard/
# Then on VPS: unzip cache.zip && rm cache.zip

# Database
scp "C:\Users\david\Downloads\momentum-dashboard\momentum-dashboard\momentum_dashboard.db" root@89.167.110.85:/root/momentum-dashboard/momentum-dashboard/

# Config (contains API key — not in git)
scp "C:\Users\david\Downloads\momentum-dashboard\momentum-dashboard\config.py" root@89.167.110.85:/root/momentum-dashboard/momentum-dashboard/
```

---

## Day-to-Day Operations

### Start / restart the server (one-liner)
```bash
pkill -f "python3 app.py" 2>/dev/null; sleep 1; cd /root/momentum-dashboard/momentum-dashboard && tmux kill-session -t dashboard 2>/dev/null; tmux new -d -s dashboard 'python3 app.py --serve-only' && echo "Server started"
```

### Check server output
```bash
tmux attach -s dashboard
# Ctrl+B then D to detach without stopping
```

### Stop the server
```bash
pkill -f "python3 app.py"
```

---

## Code Update Cycle (Local Changes → Live Server)

### 1. Make changes locally (on your Windows PC)

### 2. Push to GitHub (Windows PowerShell)
```powershell
cd "C:\Users\david\Downloads\momentum-dashboard"
git add -A && git commit -m "describe what changed" && git push
```

### 3. Pull + restart on VPS (SSH into VPS, paste this one-liner)
```bash
cd /root/momentum-dashboard && git pull && pkill -f "python3 app.py" 2>/dev/null; sleep 1; tmux kill-session -t dashboard 2>/dev/null; tmux new -d -s dashboard 'cd /root/momentum-dashboard/momentum-dashboard && python3 app.py --serve-only' && echo "Updated and restarted"
```

---

## Friend Access (Tailscale)
1. Friend installs Tailscale (tailscale.com/download)
2. Invite them via Tailscale admin panel (login.tailscale.com/admin)
3. They open http://100.97.33.3:8080 in their browser

---

## Key Files NOT in Git (must transfer manually)
- `config.py` — contains Polygon API key
- `momentum_dashboard.db` — SQLite database (~1GB)
- `cache/` — price CSV cache (~900MB)
- `*.db-journal`, `*.db-wal` — SQLite temp files

These live only on the VPS and your local machine. `git pull` never touches them.

---

## Troubleshooting

### Server won't start — missing module
```bash
pip3 install <module_name> --break-system-packages
```

### Port 8080 already in use
```bash
pkill -f "python3 app.py"
# Wait a few seconds, then start again
```

### Check if server is running
```bash
ps aux | grep app.py
```

### DB is locked (concurrent writes)
Only an issue if sweep fetch is running while someone is using the server.
Restart the server to kill the fetch thread.
