# Distributed Video Encoding System

This project implements a basic distributed system for uploading, processing (encoding/resizing), and retrieving video files using gRPC. The system consists of a master node that manages tasks and worker nodes that perform the actual video processing.

## Components

- **`replication.proto`**: Defines the gRPC services and message structures used for communication between the client, master node, and worker nodes.
- **`node.py`**: Implements the core logic for both the master and worker roles. A single instance of `node.py` can run as either a master or a worker.
  - **Master Role**: Handles client requests (upload, retrieve, status), segments large videos, distributes processing tasks to workers, collects processed shards, and concatenates them into the final output video. It also includes basic node discovery and election-related RPCs (though the full consensus logic might not be fully implemented).
  - **Worker Role**: Receives video shards from the master, processes them (e.g., resizing using FFmpeg), and provides the processed shards back to the master upon request.
- **`client.py`**: A command-line client application that interacts with the master node to upload videos for processing, check the status of processing tasks, and retrieve the final processed video. It includes a progress bar for video retrieval using `tqdm`.

## Features

- **Video Upload**: Clients can upload video files to the master node.
- **Distributed Processing**: The master distributes video processing tasks (on segmented video shards) to available worker nodes.
- **Video Retrieval**: Clients can retrieve the processed video from the master once the processing is complete.
- **Processing Status**: Clients can query the status of their video processing tasks.
- **Basic Node Roles**: Supports running nodes as either a master or a worker.
- **gRPC Streaming**: Uses gRPC for efficient streaming of video data during upload and retrieval.
- **Download Progress Bar**: The client shows a progress bar during video retrieval.

## Prerequisites

- Python 3.7+
- `pip` package installer
- FFmpeg installed and accessible in the system's PATH on both master and worker nodes (used for video segmentation, processing, and concatenation).
- `grpcio` and `protobuf` Python packages.
- `tqdm` Python package (for the client progress bar).
- `psutil` Python package (for node stats).
- `ffmpeg-python` Python package (a Python wrapper for FFmpeg).

## Setup

1.  **Install Dependencies**:

    Bash

    ```
    pip install grpcio protobuf grpcio-tools tqdm psutil ffmpeg-python

    ```

2.  **Install FFmpeg**: Ensure FFmpeg is installed on your system and available in the PATH. You can usually install it via your system's package manager (e.g., `sudo apt-get install ffmpeg` on Debian/Ubuntu, `brew install ffmpeg` on macOS).

3.  **Compile the `.proto` file**: Generate the Python gRPC code from the `replication.proto` file.

    Bash

    ```
    python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. replication.proto

    ```

    This will generate `replication_pb2.py` and `replication_pb2_grpc.py` in the same directory.

## Running the System

You need to start one node as the master and one or more nodes as workers.

1.  **Start the Master Node**:

    Bash

    ```
    python node.py --host <master_host> --port <master_port> --role master

    ```

    Replace `<master_host>` and `<master_port>` with the IP address/hostname and port the master should listen on.

2.  **Start Worker Nodes**:

    Bash

    ```
    python node.py --host <worker_host> --port <worker_port> --role worker --master <master_host>:<master_port> --nodes <master_host>:<master_port>,<other_worker_host>:<other_worker_port>,...

    ```

    Replace `<worker_host>` and `<worker_port>` with the worker's address. Replace `<master_host>:<master_port>` with the address of the running master node. The `--nodes` argument should be a comma-separated list of known node addresses, including the master and other workers. This helps nodes discover each other.

    _Example (Master on localhost:50051, Worker on localhost:50052)_:

    - Start Master: `python node.py --host localhost --port 50051 --role master`
    - Start Worker: `python node.py --host localhost --port 50052 --role worker --master localhost:50051 --nodes localhost:50051,localhost:50052`

## Using the Client

The `client.py` script provides commands for interacting with the master node.

Bash

```
python client.py --master <master_host>:<master_port> [command] [options]

```

- `<master_host>:<master_port>`: The address of the master node.

**Commands:**

- **Upload a video**:

  Bash

  ```
  python client.py --master <master_address> --upload <path_to_video_file> --width <target_width> --height <target_height> [--output <output_path_for_retrieval>]

  ```

  - `--upload`: Path to the local video file to upload.
  - `--width`: Target width for the processed video.
  - `--height`: Target height for the processed video.
  - `--output`: (Optional) If specified, the client will automatically poll for the video status after upload and retrieve the processed video to this path when complete.

- **Retrieve a processed video**:

  Bash

  ```
  python client.py --master <master_address> --retrieve <video_id> --output <output_path>

  ```

  - `--retrieve`: The ID of the video to retrieve (obtained from the upload response).
  - `--output`: The local path where the retrieved video will be saved.

- **Get video processing status**:

  Bash

  ```
  python client.py --master <master_address> --status <video_id>

  ```

  - `--status`: The ID of the video to check the status for.

## gRPC Services and Messages

The `replication.proto` file defines the following services:

- **`MasterService`**: RPCs for client-master interaction and master-side operations.
  - `UploadVideo`: Client streams video chunks to the master.
  - `RetrieveVideo`: Master streams processed video chunks to the client.
  - `GetVideoStatus`: Client requests the processing status of a video.
  - `ReportWorkerShardStatus`: Workers report the status of processed shards to the master.
- **`WorkerService`**: RPCs for master-worker interaction related to shard processing.
  - `ProcessShard`: Master sends a video shard to a worker for processing.
  - `RequestShard`: Master requests a processed shard back from a worker.
- **`NodeService`**: RPCs for inter-node communication (e.g., discovery, leader election - though election logic might be simplified or a placeholder).
  - `AnnounceMaster`: Nodes announce the current master.
  - `RequestVote`: Nodes request votes during an election.
  - `GetNodeStats`: Get resource usage and status statistics from a node.
  - `GetCurrentMaster`: Clients or other nodes can discover the current master's address.

The `.proto` file also defines various message structures used by these RPCs to carry data like video chunks, status information, shard details, and node statistics.

## Limitations and Future Improvements

- **No Real-time Streaming**: The current system downloads the entire processed video file before playback can begin. It does not support real-time adaptive streaming like HLS or DASH. Implementing this would require significant architectural changes, including video segmentation on the server and a dedicated streaming client/player.
- **Simplified Consensus**: The node discovery and leader election RPCs (`NodeService`) are defined, but the full, robust consensus algorithm (like Raft or Paxos) is likely not fully implemented in `node.py`. A production system would require a more complete and fault-tolerant consensus mechanism.
- **Error Handling and Resilience**: While some basic error handling is present, a production system would need more comprehensive error handling, retry mechanisms, and strategies for handling node failures gracefully.
- **Scalability**: The current master might become a bottleneck with a very large number of videos or workers. Further scalability improvements could involve partitioning the master's responsibilities or using a distributed queue.
- **Security**: The current implementation uses insecure gRPC channels. For production, secure channels with authentication and encryption would be necessary.
- **Monitoring and Logging**: More detailed logging and monitoring would be beneficial for debugging and operating the system.
- **Configuration Management**: Using configuration files instead of command-line arguments for complex deployments would be more manageable.
- **Video Format Support**: FFmpeg provides broad support, but ensuring compatibility and handling various codecs and containers robustly is important.
