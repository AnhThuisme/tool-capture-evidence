# Local Web + Cloudflare Tunnel + Custom Domain

Use this when you need:
- your own domain, for example `tool.fanscom.vn`
- Chrome ports and Facebook login sessions to persist
- start/replay jobs on the same Windows machine that opened Chrome

## Why this works

Everything runs on the same machine:
- local web UI
- Chrome windows by port
- Facebook session/cookies
- evidence runner

Only the public URL goes through Cloudflare Tunnel.

## One-time setup

### 1. Install `cloudflared`

Example with `winget`:

```powershell
winget install --id Cloudflare.cloudflared
```

### 2. Create a Cloudflare Tunnel in the dashboard

In Cloudflare dashboard:
- open your zone/domain
- open `Networks` -> `Tunnels`
- create a `Cloudflared` tunnel
- add a public hostname, for example:
  - `tool.fanscom.vn`
- set the service target to:
  - `http://localhost:8012`

Copy the tunnel token from Cloudflare.

### 3. Create local env file

Create:

`cloudflare_tunnel.env`

with:

```env
CLOUDFLARE_TUNNEL_TOKEN=your_tunnel_token_here
CLOUDFLARE_PUBLIC_HOSTNAME=tool.fanscom.vn
```

This file is ignored by Git.

## Start

```powershell
.\setup_windows.ps1
.\run_web_cloudflare_tunnel.ps1
```

The script will:
- start the local web app on `8012`
- start `cloudflared`
- print your local URL
- print your custom domain URL

## Important

- Keep the PowerShell window open while you use the site.
- Open/login Chrome from the same machine that runs this script.
- Start and replay jobs from the same custom domain URL.
- Do not use Railway for job execution if you want the local Chrome login session to persist.

## Stop

The script prints both process IDs:
- web
- cloudflared

Stop them with:

```powershell
Stop-Process -Id <web_pid>,<cloudflared_pid>
```
