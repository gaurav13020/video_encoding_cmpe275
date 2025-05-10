# Worker Health Check System

This document provides a detailed explanation of the worker health check system implemented in the Video Encoding project, including guidance for master node implementation.

## Overview

The health check system enables the master node to monitor the health and performance status of worker nodes within the distributed video encoding system. This is critical for ensuring encoding tasks are only assigned to responsive, healthy workers.

## Components

### 1. Protocol Definition

The health check functionality is defined in `replication.proto` with the following messages:

```protobuf
message HealthCheckRequest {
  string master_id = 1; // Identifier of the master requesting the health check
}

message HealthCheckResponse {
  bool is_healthy = 1; // Whether the worker is healthy
  string worker_id = 2; // Identifier of the worker responding
  float cpu_utilization = 3; // Current CPU utilization (0-100)
  int64 memory_usage_bytes = 4; // Current memory usage in bytes
  int32 active_tasks = 5; // Number of tasks currently being processed
  string message = 6; // Additional health information or error message
}
```

The `CheckHealth` RPC is added to the `VideoProcessingService`:

```protobuf
service VideoProcessingService {
  // Existing RPCs
  rpc ProcessChunk (ProcessChunkRequest) returns (ProcessChunkResponse);
  rpc GetShard (GetShardRequest) returns (GetShardResponse);
  
  // Health check RPC
  rpc CheckHealth (HealthCheckRequest) returns (HealthCheckResponse);
}
```

### 2. Worker-Side Implementation

The worker node implements the `CheckHealth` RPC in `worker.py` to:
- Report its current health status
- Provide system metrics (CPU, memory usage)
- Report the number of active encoding tasks
- Return additional status information

```python
async def CheckHealth(self, request: replication_pb2.HealthCheckRequest, context: grpc.aio.ServicerContext) -> replication_pb2.HealthCheckResponse:
    """Handles health check requests from the master."""
    master_id = request.master_id
    logging.info(f"[{self.worker_id}] Received health check from master '{master_id}'")
    
    try:
        # Get system metrics using psutil
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory_usage = psutil.Process(os.getpid()).memory_info().rss  # Resident Set Size in bytes
        
        # Get number of active tasks
        active_tasks = 0
        async with self._active_tasks_lock:
            active_tasks = self._active_tasks
            
        # Create health check response
        response = replication_pb2.HealthCheckResponse(
            is_healthy=True,  # Default to healthy
            worker_id=self.worker_id,
            cpu_utilization=cpu_percent,
            memory_usage_bytes=memory_usage,
            active_tasks=active_tasks,
            message="Worker is healthy and ready to process tasks"
        )
        
        return response
        
    except Exception as e:
        logging.error(f"[{self.worker_id}] Error during health check: {e}", exc_info=True)
        return replication_pb2.HealthCheckResponse(
            is_healthy=False,
            worker_id=self.worker_id,
            cpu_utilization=0.0,
            memory_usage_bytes=0,
            active_tasks=0,
            message=f"Health check failed: {str(e)}"
        )
```

### 3. Master-Side Implementation Sample

The project includes a sample health monitoring implementation in `test_health_check.py`, which demonstrates how a master node should monitor workers:

- Periodically poll all registered workers
- Track worker health status over time
- Apply threshold-based marking to identify unhealthy nodes
- Display a cluster health status dashboard

## Guidance for Master Node Implementation

When implementing a master node for this system, you should incorporate the following health check functionality:

### 1. Worker Registry

Maintain a registry of worker nodes including:
- Worker address (host:port)
- Current health status
- Last seen timestamp
- Performance metrics history
- Number of consecutive failed checks

Example registry structure:
```python
workers = {
    "worker_address": {
        "healthy": True,
        "last_seen": timestamp,
        "failed_checks": 0,
        "stats": {
            "cpu": 12.5,
            "memory": 104857600,  # 100 MB
            "active_tasks": 3
        }
    }
}
```

### 2. Health Check Scheduler

Implement a periodic health check scheduler that:
- Runs in its own background task/thread
- Polls all workers at configurable intervals (5-10 seconds recommended)
- Updates the worker registry with results
- Logs significant health status changes

```python
async def start_health_monitoring(self, interval=5.0):
    """Start a background task for health monitoring."""
    self._monitor_task = asyncio.create_task(self._monitor_loop(interval))
    
async def _monitor_loop(self, interval):
    """Main monitoring loop."""
    while True:
        await self._check_all_workers()
        await asyncio.sleep(interval)
```

### 3. Worker Health Assessment

Develop a health assessment algorithm that:
- Marks workers as unhealthy after multiple consecutive failed checks
- Considers both connectivity failures and self-reported unhealthy status
- Provides grace periods for temporary issues (2-3 failed checks before marking down)

```python
def _assess_worker_health(self, worker_address, check_result):
    # If check succeeded, reset failure counter
    if check_result.is_healthy:
        self.workers[worker_address]["healthy"] = True
        self.workers[worker_address]["failed_checks"] = 0
        return
        
    # Otherwise increment failure counter
    self.workers[worker_address]["failed_checks"] += 1
    
    # Mark as unhealthy after threshold
    if self.workers[worker_address]["failed_checks"] >= self.unhealthy_threshold:
        self.workers[worker_address]["healthy"] = False
```

### 4. Task Allocation Strategy

Implement a task allocation strategy that:
- Only assigns encoding tasks to healthy workers
- Considers worker load (active_tasks, cpu_utilization) when distributing work
- Re-routes tasks from workers that become unhealthy
- Implements back-pressure when worker pool health degrades

```python
def get_available_worker(self):
    """Find the best worker for a new encoding task."""
    healthy_workers = [w for w in self.workers.items() if w[1]["healthy"]]
    
    if not healthy_workers:
        raise NoHealthyWorkersError("No healthy workers available")
        
    # Sort by load (active tasks + CPU utilization)
    sorted_workers = sorted(
        healthy_workers, 
        key=lambda w: w[1]["stats"]["active_tasks"] + (w[1]["stats"]["cpu"] / 10.0)
    )
    
    # Return address of least loaded worker
    return sorted_workers[0][0]
```

### 5. Fault Tolerance

Design for fault tolerance by:
- Implementing timeouts on health check requests (2-3 seconds recommended)
- Catching and handling connection errors gracefully
- Providing fallback mechanisms when workers fail
- Considering redundant task assignments for critical workloads

### 6. Metrics and Monitoring

Extend the monitoring system to:
- Record historical health data for trend analysis
- Generate alerts on significant health status changes
- Provide a dashboard or status endpoint for system health
- Track worker latency and performance over time

## Testing Health Checks

For testing the health check functionality:

1. Start multiple worker instances:
   ```bash
   ./start_worker.sh --port=50061
   ./start_worker.sh --port=50062
   ```

2. Run the health check monitor sample:
   ```bash
   ./start_health_checker.sh --master_id=master-1
   ```

3. Simulate failures by:
   - Stopping worker processes
   - Blocking network access
   - Creating high CPU load on workers

## Performance Considerations

- Health checks should be lightweight and quick to avoid overloading workers
- Use an adaptive interval based on cluster size (more workers = less frequent checks)
- Consider implementing push-based health updates for large clusters
- Buffer health status changes to avoid flapping (rapid healthy/unhealthy transitions)


This health check system provides the foundation for building a reliable distributed video encoding system that can handle worker failures gracefully. 