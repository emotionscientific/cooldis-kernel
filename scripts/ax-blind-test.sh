#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="generate"
OUT_DIR=""

usage() {
  cat <<'USAGE'
usage: scripts/ax-blind-test.sh [--run] [--out DIR]

Generates an Agent Experience blind-test prompt. With --run, spawns a separate
agent command to answer it and writes answers.md for review.

Environment:
  VERLET_AX_AGENT_COMMAND   command that reads the prompt from stdin and writes answers to stdout

If --run is used and VERLET_AX_AGENT_COMMAND is unset, the script tries:
  codex exec
USAGE
}

while (($# > 0)); do
  case "$1" in
    --run)
      MODE="run"
      shift
      ;;
    --out)
      OUT_DIR="${2:-}"
      if [[ -z "$OUT_DIR" ]]; then
        usage >&2
        exit 2
      fi
      shift 2
      ;;
    --help | -h)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="${TMPDIR:-/tmp}/verlet-ax-blind-test-$(date +%Y%m%d-%H%M%S)"
fi

mkdir -p "$OUT_DIR"

QUESTIONS="$OUT_DIR/questions.md"
PROMPT="$OUT_DIR/prompt.md"
ANSWERS="$OUT_DIR/answers.md"

cat >"$QUESTIONS" <<'QUESTIONS'
# Verlet AX Blind-Test Care Test

Answer these as a fresh coding agent with no private context. Cite repo files for
each answer.

1. Why should I give a shit about Verlet?
2. What painful problem does Verlet solve for someone building agents today?
3. What does "define the agent, not the app around it" mean?
4. Why might someone describe this as Vercel for agents, even if the docs do not lead with that phrase?
5. What does "managed agent platform without vendor lock-in" mean here?
6. What can I do with Verlet that is hard or annoying with normal agent frameworks?
7. Who is Verlet for right now?
8. What would make me reach for Verlet instead of just writing another agent app?
9. If I already have LangGraph, Mastra, MCP tools, or a sandbox platform, why would Verlet matter?
10. What is the simplest thing I can try locally?
11. What does "install agents like packages" actually mean for a user?
12. What does "govern agents like infrastructure" mean in plain language?
13. What is real in the repo today, and what is still future direction?
14. What would a business team, platform team, or developer get from Verlet?
15. What are the biggest current gaps or risks?
16. What should I read first if I have ten minutes or want to understand the code?
17. Give me the blunt, non-hype one-paragraph pitch.
QUESTIONS

cat >"$PROMPT" <<PROMPT
You are a separate blind-test coding agent in the Verlet repository.

Working directory:
$ROOT

Task:
Answer the FAQ below by inspecting only the repository. Do not assume private
thread context. Prefer the public docs in docs/ first, then AGENTS.md,
README.md, docs/README.md, docs/index.md, docs/public-api-coverage.md,
docs/abi.md, docs/agent-cli.md, docs/daemon.md,
docs/app-server.md, and docs/testing-guidelines.md when relevant.

Output format:
- Answer each numbered question.
- Keep answers concise, plainspoken, and user-facing.
- Cite at least one repo file path for each answer.
- Say "not found" if the repo does not support an answer.

$(cat "$QUESTIONS")
PROMPT

printf 'wrote questions: %s\n' "$QUESTIONS"
printf 'wrote prompt: %s\n' "$PROMPT"

if [[ "$MODE" != "run" ]]; then
  printf 'run with --run to spawn an answering agent.\n'
  exit 0
fi

agent_command="${VERLET_AX_AGENT_COMMAND:-}"
if [[ -z "$agent_command" ]]; then
  if command -v codex >/dev/null 2>&1; then
    agent_command="codex exec"
  else
    printf 'error: --run requires VERLET_AX_AGENT_COMMAND or codex on PATH\n' >&2
    exit 1
  fi
fi

(
  cd "$ROOT"
  bash -lc "$agent_command" <"$PROMPT" >"$ANSWERS"
)

printf 'wrote answers: %s\n' "$ANSWERS"
