import grpc
import asyncio
import os
import argparse
import logging
import time
import random # For jitter or random selection if needed, not used in current simple load balancing

# Assuming replication_pb2 and replication_pb2_grpc are in the same directory or accessible in PYTHONPATH
import replication_pb2
import replication_pb2_grpc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration for the Master
CHUNK_SIZE = 1024 * 1024  # 1MB chunks, can be adjusted
HEALTH_CHECK_INTERVAL = 10  # seconds
MASTER_ID = "master-load-balancer-01"

# Global dictionary to store worker status and load information
# Structure: { "worker_address": {"healthy": bool, "active_tasks": int, "cpu_utilization": float, "last_seen": float, "failed_checks": int} }
worker_status_map = {}
worker_status_lock = asyncio.Lock() # To protect concurrent access to worker_status_map

async def get_worker_health(worker_address):
    """Fetches health status from a single worker."""
    try:
        async with grpc.aio.insecure_channel(worker_address) as channel:
            stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
            request = replication_pb2.HealthCheckRequest(master_id=MASTER_ID)
            response = await stub.CheckHealth(request, timeout=5.0)
            async with worker_status_lock:
                worker_status_map[worker_address] = {
                    "healthy": response.is_healthy,
                    "active_tasks": response.active_tasks,
                    "cpu_utilization": response.cpu_utilization,
                    "last_seen": time.monotonic(),
                    "failed_checks": 0
                }
            if not response.is_healthy:
                logging.warning(f"Worker {worker_address} reported unhealthy: {response.message}")
            return True
    except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
        logging.warning(f"Health check failed for worker {worker_address}: {e}")
        async with worker_status_lock:
            if worker_address not in worker_status_map:
                 worker_status_map[worker_address] = {"healthy": False, "active_tasks": float('inf'), "cpu_utilization": float('inf'), "last_seen": 0, "failed_checks": 0}
            worker_status_map[worker_address]["healthy"] = False
            worker_status_map[worker_address]["failed_checks"] = worker_status_map[worker_address].get("failed_checks", 0) + 1
        return False
    except Exception as e:
        logging.error(f"Unexpected error during health check for {worker_address}: {e}")
        async with worker_status_lock:
            if worker_address not in worker_status_map:
                 worker_status_map[worker_address] = {"healthy": False, "active_tasks": float('inf'), "cpu_utilization": float('inf'), "last_seen": 0, "failed_checks": 0}
            worker_status_map[worker_address]["healthy"] = False
            worker_status_map[worker_address]["failed_checks"] = worker_status_map[worker_address].get("failed_checks", 0) + 1
        return False

async def update_all_worker_statuses_periodically(worker_addresses):
    """Periodically updates the health and load status of all workers."""
    logging.info("Starting periodic worker health checks...")
    while True:
        tasks = [get_worker_health(addr) for addr in worker_addresses]
        await asyncio.gather(*tasks)
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)

async def select_best_worker():
    """Selects the best worker based on health and load (active_tasks, then cpu)."""
    async with worker_status_lock:
        if not worker_status_map:
            return None
        healthy_workers = []
        for addr, status in worker_status_map.items():
            if status.get("healthy", False):
                healthy_workers.append({
                    "address": addr,
                    "active_tasks": status.get("active_tasks", float('inf')),
                    "cpu_utilization": status.get("cpu_utilization", float('inf'))
                })
        if not healthy_workers:
            logging.warning("No healthy workers available.")
            return None
        healthy_workers.sort(key=lambda w: (w["active_tasks"], w["cpu_utilization"]))
        selected = healthy_workers[0]["address"]
        logging.info(f"Selected worker {selected} with {healthy_workers[0]['active_tasks']} tasks, {healthy_workers[0]['cpu_utilization']:.2f}% CPU.")
        return selected

async def send_chunk_to_worker(worker_address, video_id, chunk_index, chunk_data):
    """Sends a single chunk to a worker and awaits response."""
    try:
        async with grpc.aio.insecure_channel(worker_address) as channel:
            stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
            request = replication_pb2.ProcessChunkRequest(
                video_id=video_id,
                chunk_index=chunk_index,
                chunk_data=chunk_data
            )
            response = await stub.ProcessChunk(request, timeout=30.0)
            if response.success:
                logging.info(f"Chunk {chunk_index} for {video_id} successfully processed by {worker_address}. Shard: {response.shard_location}")
                return True, response.shard_location
            else:
                logging.error(f"Worker {worker_address} failed to process chunk {chunk_index} for {video_id}: {response.message}")
                return False, None
    except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
        logging.error(f"Error sending chunk {chunk_index} to worker {worker_address}: {e}")
        async with worker_status_lock:
            if worker_address in worker_status_map:
                worker_status_map[worker_address]["healthy"] = False 
                worker_status_map[worker_address]["failed_checks"] = worker_status_map[worker_address].get("failed_checks", 0) + 1
        return False, None
    except Exception as e:
        logging.error(f"Unexpected error sending chunk {chunk_index} to worker {worker_address}: {e}")
        return False, None

async def distribute_video_to_workers(video_path, worker_addresses):
    """Reads a video file, chunks it, and distributes chunks to workers using load-aware strategy."""
    video_id = os.path.basename(video_path)
    logging.info(f"Starting load-aware distribution of video '{video_id}' to workers: {worker_addresses}")
    if not os.path.exists(video_path):
        logging.error(f"Video file not found: {video_path}")
        return {}
    if not worker_addresses:
        logging.error("No worker addresses provided.")
        return {}
    async with worker_status_lock:
        for addr in worker_addresses:
            if addr not in worker_status_map:
                worker_status_map[addr] = {"healthy": False, "active_tasks": float('inf'), "cpu_utilization": float('inf'), "last_seen": 0, "failed_checks": 0}
    health_check_task = asyncio.create_task(update_all_worker_statuses_periodically(worker_addresses))
    chunk_index = 0
    results = {}
    no_worker_retry_delay = 5
    try:
        with open(video_path, "rb") as f:
            while True:
                chunk_data = f.read(CHUNK_SIZE)
                if not chunk_data:
                    break
                selected_worker = None
                while selected_worker is None:
                    selected_worker = await select_best_worker()
                    if selected_worker is None:
                        logging.warning(f"No suitable worker available for chunk {chunk_index}. Waiting for {no_worker_retry_delay}s...")
                        await asyncio.sleep(no_worker_retry_delay)
                    else:
                        break
                logging.info(f"Sending chunk {chunk_index} of {video_id} to selected worker {selected_worker}")
                async with worker_status_lock:
                    if selected_worker in worker_status_map and worker_status_map[selected_worker]["healthy"]:
                         worker_status_map[selected_worker]["active_tasks"] += 1
                success, shard_location = await send_chunk_to_worker(selected_worker, video_id, chunk_index, chunk_data)
                results[f"{video_id}_chunk_{chunk_index}"] = {
                    "worker": selected_worker,
                    "status": "success" if success else "failed",
                    "shard_location": shard_location if success else None
                }
                if not success:
                    logging.warning(f"Failed to process chunk {chunk_index} on worker {selected_worker}.")
                chunk_index += 1
    except FileNotFoundError:
        logging.error(f"Error: Video file not found at {video_path}")
    except Exception as e:
        logging.error(f"An error occurred during video processing and distribution: {e}", exc_info=True)
    finally:
        logging.info("Stopping periodic health checks...")
        health_check_task.cancel()
        try:
            await health_check_task
        except asyncio.CancelledError:
            logging.info("Health check task cancelled successfully.")
        except Exception as e:
            logging.error(f"Error during health_check_task cleanup: {e}")
    logging.info(f"Finished distributing video '{video_id}'. Processed {chunk_index} chunks.")
    return results

async def main():
    global CHUNK_SIZE # Moved global declaration to the top of the function

    parser = argparse.ArgumentParser(description="Master process for distributing video chunks to workers (load-aware).")
    parser.add_argument('--video_path', type=str, required=True, help='Path to the video file to be processed.')
    parser.add_argument('--workers', type=str, required=True, help='Comma-separated list of worker addresses (e.g., localhost:50061,localhost:50062).')
    parser.add_argument('--chunk_size', type=int, default=CHUNK_SIZE, help=f'Size of each chunk in bytes (default: {CHUNK_SIZE}).')

    args = parser.parse_args()

    CHUNK_SIZE = args.chunk_size # This now correctly modifies the global CHUNK_SIZE

    worker_addresses = [addr.strip() for addr in args.workers.split(',') if addr.strip()]

    if not worker_addresses:
        print("No worker addresses provided. Exiting.")
        return

    print(f"Master started (load-aware). Video: {args.video_path}, Workers: {worker_addresses}, Chunk Size: {CHUNK_SIZE}")
    
    distribution_results = await distribute_video_to_workers(args.video_path, worker_addresses)
    
    print("\n--- Distribution Summary ---")
    if distribution_results:
        for chunk_id, result in distribution_results.items():
            print(f"Chunk: {chunk_id}, Worker: {result['worker']}, Status: {result['status']}, Shard: {result['shard_location']}")
    else:
        print("No chunks were processed or distribution failed early.")
    print("---------------------------")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMaster process interrupted.")
    except Exception as e:
        logging.error(f"Master process failed: {e}", exc_info=True)

