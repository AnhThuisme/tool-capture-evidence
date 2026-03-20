# Local Web + Tunnel

This mode keeps everything on the same Windows machine:
- Chrome login windows
- Facebook session/cookies
- job start / replay / screenshot runner

Use this when you need:
- open Chrome by port
- log in once
- re-run later without losing the login session

## Start

```powershell
.\setup_windows.ps1
.\run_web_tunnel.ps1
```

The script will:
- start the local web app
- start `ngrok`
- print both local and public URLs
- open the public URL in your browser

## Important

- Keep the PowerShell window open while you use the web.
- Open/login Chrome from the same machine that runs the script.
- Start and replay jobs from the same local/tunnel web.
- Do not use Railway for the run itself if you want local Chrome login sessions to persist.

## Stop

The script prints the process IDs for:
- web
- ngrok

Stop them with:

```powershell
Stop-Process -Id <web_pid>,<ngrok_pid>
```
