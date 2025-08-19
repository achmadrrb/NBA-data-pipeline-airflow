initial-setup:
	uv sync
	uv run pre-commit install

clean-cache:
	uv run cleanpy --all --exclude-envs .
