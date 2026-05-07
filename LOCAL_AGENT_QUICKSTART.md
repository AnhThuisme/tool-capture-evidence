# Local Agent Quickstart (One Click)

For deployed web usage: this opens local Chrome debug and starts local agent on your computer.

## Windows
1. Double-click `run_local_agent.bat`.
2. It will auto:
- rebuild `.venv` if broken,
- install dependencies,
- open Chrome debug on port `9223` (with PNA unblock flag),
- start local agent at `http://127.0.0.1:8765`.
3. Keep the window open.
4. Use deployed web and click `Lauch Chrome` or run job.

## macOS
1. Double-click `run_local_agent.command`.
2. It will auto:
- rebuild `.venv` if broken,
- install dependencies,
- open Chrome debug on port `9223` (with PNA unblock flag),
- start local agent at `http://127.0.0.1:8765`.
3. Keep the terminal window open.
4. Use deployed web and click `Lauch Chrome` or run job.

## Verify
Open this on the same machine:
`http://127.0.0.1:8765/health`

If you see JSON with `"ok": true`, setup is ready.
