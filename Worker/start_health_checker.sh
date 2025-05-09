#!/bin/bash
source venv/bin/activate
python test_health_check.py --workers=localhost:50061,localhost:50062,localhost:50063 "$@" 