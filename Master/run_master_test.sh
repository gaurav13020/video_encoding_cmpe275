#!/bin/bash

# This script runs the unit tests for master.py
# It assumes it is being run from the video_encoding_cmpe275-main/Master/ directory

# Activate virtual environment if it exists and is used by the project
# If venv is in Worker directory as per previous setup:
WORKER_DIR="../Worker"
if [ -d "$WORKER_DIR/venv" ]; then
    echo "Activating virtual environment from Worker directory..."
    source "$WORKER_DIR/venv/bin/activate"
else
    echo "Virtual environment not found in Worker directory, proceeding with system Python."
fi

python3 test_master.py

# Deactivate virtual environment if it was activated
if [ -d "$WORKER_DIR/venv" ]; then
    deactivate
fi

