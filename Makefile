.PHONY: audit build check format test typecheck

check: format typecheck test audit build

format:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy src

test:
	uv run pytest --cov --cov-report=term-missing --cov-fail-under=80

audit:
	@set -eu; \
		audit_file="$$(mktemp "$${TMPDIR:-/tmp}/atlas-research-audit.XXXXXX")"; \
		trap 'rm -f "$$audit_file"' EXIT HUP INT TERM; \
		uv export --frozen --all-groups --no-emit-project \
			--output-file "$$audit_file" >/dev/null; \
		uv run pip-audit --strict --disable-pip --require-hashes --no-deps \
			-r "$$audit_file"

build:
	uv build
