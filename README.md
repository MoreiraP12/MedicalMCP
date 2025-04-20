# MedicalMCP

https://modelcontextprotocol.io/quickstart/server

# Create a new directory for our project
uv init mimic
cd mimic

# Create virtual environment and activate it
uv venv
source .venv/bin/activate

# Install dependencies
uv add "mcp[cli]" httpx

# Create our server file
touch mimic.py


