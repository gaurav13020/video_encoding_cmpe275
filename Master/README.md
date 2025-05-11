## Master Process (`master.py`)

This directory contains the `master.py` script, which acts as the Master process responsible for orchestrating the distribution of video chunks to available Worker nodes.

### Key Features:
- **Video Input**: Takes a path to a video file as input.
- **Worker Registration**: Accepts a list of worker node addresses (host:port).
- **Video Chunking**: Divides the input video into smaller, manageable chunks of a configurable size (default 1MB).
- **Load-Aware Distribution**: Implements a load-aware strategy for distributing chunks. It periodically queries the health and load (active tasks, CPU utilization) of each worker using the `CheckHealth` RPC.
- **Worker Selection**: Based on the load information, it selects the least busy worker (prioritizing fewer active tasks, then lower CPU) to send the next video chunk via the `ProcessChunk` RPC.
- **Error Handling**: Includes basic error management for worker communication. If a worker is unresponsive, it is temporarily marked as unavailable, and the master will periodically attempt to re-check its status.
- **Logging**: Provides detailed logs of its operations, including worker selection, chunk distribution, and any errors encountered.
- **Sample Video**: A sample video (`sample_videos/file_example_MP4_1920_18MG.mp4`) is included for testing and demonstration.

### Associated Scripts:

- **`start_master.sh`**: A convenience script to start the `master.py` process. By default, it uses the included sample video (`./sample_videos/file_example_MP4_1920_18MG.mp4`) and attempts to connect to workers on `localhost:50061` and `localhost:50062`. You can modify this script or pass arguments to it if needed (though the current version has hardcoded values for simplicity).

- **`test_master.py`**: A Python script using the `unittest` framework to test the functionality of `master.py`. It automatically starts two worker instances, runs `master.py` with the sample video, and checks for successful completion and shard creation.

- **`run_master_test.sh`**: A shell script to execute `test_master.py`. It handles activating the Python virtual environment (expected to be in the `../Worker/venv` directory) before running the tests.

### How to Run `master.py`

#### 1. Using the Startup Script (Recommended for Sample Video):

   This is the easiest way to run the master with the provided sample video.

   1.  Ensure you have at least two instances of `worker.py` (from the `../Worker` directory) running and accessible on `localhost:50061` and `localhost:50062`. You can use the `../Worker/start_worker.sh` script for this (run it twice with different port arguments if needed, or modify it to launch multiple workers).
   2.  Make sure you are in the `video_encoding_cmpe275-main/Master` directory.
   3.  Activate the Python virtual environment (located in the `../Worker` directory):
       ```bash
       source ../Worker/venv/bin/activate
       ```
   4.  Execute the startup script:
       ```bash
       ./start_master.sh
       ```
       This will run `master.py` with `./sample_videos/file_example_MP4_1920_18MG.mp4` and workers `localhost:50061,localhost:50062`.

#### 2. Running the Automated Tests:

   The test script will automatically handle starting worker processes for the duration of the test.

   1.  Make sure you are in the `video_encoding_cmpe275-main/Master` directory.
   2.  Execute the test runner script:
       ```bash
       ./run_master_test.sh
       ```
       This will activate the virtual environment, run `test_master.py`, which starts its own worker instances, tests `master.py` with the sample video, and then shuts down the workers it started.

#### 3. Manual Execution of `master.py`:

   1.  Ensure you have one or more instances of `worker.py` (from the `../Worker` directory) running and accessible.
   2.  Make sure you are in the `video_encoding_cmpe275-main/Master` directory.
   3.  Activate the Python virtual environment (located in the `../Worker` directory):
       ```bash
       source ../Worker/venv/bin/activate
       ```
   4.  Execute `master.py` with the necessary command-line arguments:

       ```bash
       python master.py --video_path <path_to_video> --workers <worker1_address>,<worker2_address>,... [--chunk_size <size_in_bytes>]
       ```
       **Example with the included sample video:**
       ```bash
       python master.py --video_path ./sample_videos/file_example_MP4_1920_18MG.mp4 --workers localhost:50061,localhost:50062
       ```

       **Arguments:**
       -   `--video_path`: (Required) Path to the video file you want to process.
       -   `--workers`: (Required) A comma-separated list of worker addresses (e.g., `localhost:50061,localhost:50062`).
       -   `--chunk_size` (Optional): The size of each video chunk in bytes. Defaults to 1MB (1048576 bytes).

The `master.py` script will then proceed to chunk the video and distribute these chunks to the specified workers based on their current load. The processed video shards will be stored in the `video_shards` directory on each respective worker machine (relative to where that `worker.py` instance is running).

