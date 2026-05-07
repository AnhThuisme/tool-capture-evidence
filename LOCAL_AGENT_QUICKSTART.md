# Local Agent Quickstart (No Terminal Workflow)

Use this when the web app is deployed on server but Chrome must open on your own computer.

## Windows
1. Double-click `run_local_agent.bat`.
2. Wait until you see `local agent => http://127.0.0.1:8765`.
3. Keep that window open.
4. Open the deployed web app and click `Lauch Chrome`.

## macOS
1. Double-click `run_local_agent.command`.
2. Wait until you see `local agent => http://127.0.0.1:8765`.
3. Keep that Terminal window open.
4. Open the deployed web app and click `Lauch Chrome`.

## Verify
Open this URL on the same machine:
`http://127.0.0.1:8765/health`

If you see JSON with `"ok": true`, the local agent is ready.
