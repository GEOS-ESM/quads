BootStrap: docker
From: python:3.11-slim

%help
    Usage: singularity run quads.sif <command>

%environment
    export PATH=/opt/venv/bin:$PATH

%post
    # Install system dependencies (if needed)
    apt-get update && apt-get install -y \
        build-essential \
        && rm -rf /var/lib/apt/lists/*

    # Create virtual environment
    python -m venv /opt/venv
    source /opt/venv/bin/activate

    # Upgrade pip and install package
    python -m pip install -U pip
    python -m pip install -e /repo

%files
    . /repo

%runscript
    exec "$@"
