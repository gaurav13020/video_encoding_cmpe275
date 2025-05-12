#!/bin/bash
# To run as ingestion server: ./start_master.sh --serve
python3 master.py --video_path ./sample_videos/file_example_MP4_1920_18MG.mp4 --workers localhost:50061,localhost:50062
