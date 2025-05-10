#!/usr/bin/env python3
# test_health_check.py
# Demonstrates how a master can monitor worker health status

import asyncio
import grpc
import replication_pb2
import replication_pb2_grpc
import argparse
import logging
import time
import sys
from typing import List, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WorkerHealthMonitor:
    """Simulates a master that monitors worker health."""
    
    def __init__(self, master_id: str):
        self.master_id = master_id
        self.workers = {}  # Dict to track worker status: {worker_address: {"last_seen": timestamp, "healthy": bool, "stats": {}}}
        self.check_interval = 5  # seconds between health checks
        self.unhealthy_threshold = 2  # Number of failed checks before marking worker as down
        
    def register_worker(self, worker_address: str):
        """Add a worker to be monitored."""
        self.workers[worker_address] = {
            "address": worker_address,
            "last_seen": 0,
            "healthy": False,
            "failed_checks": 0,
            "stats": {}
        }
        logging.info(f"[Master {self.master_id}] Registered worker: {worker_address}")
        
    async def check_worker_health(self, worker_address: str) -> bool:
        """Check health of a single worker."""
        try:
            # Create gRPC channel and stub for this worker
            async with grpc.aio.insecure_channel(worker_address) as channel:
                stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
                
                # Prepare health check request
                request = replication_pb2.HealthCheckRequest(
                    master_id=self.master_id
                )
                
                # Send health check with timeout
                start_time = time.monotonic()
                try:
                    response = await asyncio.wait_for(
                        stub.CheckHealth(request), 
                        timeout=3.0  # 3 second timeout
                    )
                    latency = (time.monotonic() - start_time) * 1000  # ms
                    
                    # Process response
                    if response.is_healthy:
                        # Update worker status
                        self.workers[worker_address]["healthy"] = True
                        self.workers[worker_address]["last_seen"] = time.time()
                        self.workers[worker_address]["failed_checks"] = 0
                        self.workers[worker_address]["stats"] = {
                            "cpu": response.cpu_utilization,
                            "memory": response.memory_usage_bytes,
                            "active_tasks": response.active_tasks,
                            "latency": latency,
                            "message": response.message
                        }
                        logging.info(f"[Master {self.master_id}] Worker {worker_address} is HEALTHY - CPU: {response.cpu_utilization:.1f}%, "
                                     f"Memory: {response.memory_usage_bytes/1024/1024:.1f}MB, Tasks: {response.active_tasks}, "
                                     f"Latency: {latency:.2f}ms")
                        return True
                    else:
                        logging.warning(f"[Master {self.master_id}] Worker {worker_address} reported unhealthy: {response.message}")
                        self._handle_worker_failure(worker_address, f"Worker reported unhealthy: {response.message}")
                        return False
                        
                except asyncio.TimeoutError:
                    logging.warning(f"[Master {self.master_id}] Health check timed out for worker {worker_address}")
                    self._handle_worker_failure(worker_address, "Health check timed out")
                    return False
                    
        except grpc.aio.AioRpcError as e:
            logging.warning(f"[Master {self.master_id}] gRPC error checking health of {worker_address}: {e.details()} (Code: {e.code()})")
            self._handle_worker_failure(worker_address, f"gRPC error: {e.code()}")
            return False
            
        except Exception as e:
            logging.error(f"[Master {self.master_id}] Unexpected error checking health of {worker_address}: {e}")
            self._handle_worker_failure(worker_address, f"Unexpected error: {str(e)}")
            return False
    
    def _handle_worker_failure(self, worker_address: str, reason: str):
        """Update status when a worker health check fails."""
        self.workers[worker_address]["failed_checks"] += 1
        
        if self.workers[worker_address]["failed_checks"] >= self.unhealthy_threshold:
            if self.workers[worker_address]["healthy"]:  # Only log the transition
                logging.error(f"[Master {self.master_id}] Worker {worker_address} is now marked as DOWN. Reason: {reason}")
            self.workers[worker_address]["healthy"] = False
        else:
            logging.warning(f"[Master {self.master_id}] Worker {worker_address} health check failed ({self.workers[worker_address]['failed_checks']}/"
                          f"{self.unhealthy_threshold}): {reason}")
    
    async def monitor_workers(self):
        """Main monitoring loop - continuously checks all registered workers."""
        logging.info(f"[Master {self.master_id}] Starting health monitoring for {len(self.workers)} workers")
        
        try:
            while True:
                # Get a list of all worker addresses
                worker_addresses = list(self.workers.keys())
                
                if not worker_addresses:
                    logging.warning(f"[Master {self.master_id}] No workers registered for monitoring")
                    await asyncio.sleep(self.check_interval)
                    continue
                
                # Check all workers in parallel
                health_check_tasks = []
                for worker_address in worker_addresses:
                    task = asyncio.create_task(self.check_worker_health(worker_address))
                    health_check_tasks.append(task)
                
                # Wait for all health checks to complete
                await asyncio.gather(*health_check_tasks, return_exceptions=True)
                
                # Calculate statistics
                total_workers = len(self.workers)
                healthy_workers = sum(1 for w in self.workers.values() if w["healthy"])
                
                logging.info(f"[Master {self.master_id}] Health check completed: {healthy_workers}/{total_workers} workers healthy")
                
                # Display detailed cluster health
                self.print_cluster_health()
                
                # Wait before next check
                await asyncio.sleep(self.check_interval)
                
        except asyncio.CancelledError:
            logging.info(f"[Master {self.master_id}] Health monitoring cancelled")
        except Exception as e:
            logging.error(f"[Master {self.master_id}] Unexpected error in health monitoring: {e}", exc_info=True)
    
    def print_cluster_health(self):
        """Print a summary of the cluster health."""
        print("\n" + "=" * 80)
        print(f"CLUSTER HEALTH STATUS - Master: {self.master_id} - {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Table header
        print(f"{'WORKER ADDRESS':<25} {'STATUS':<10} {'CPU %':<8} {'MEMORY':<10} {'TASKS':<8} {'LATENCY':<10} {'LAST SEEN':<20}")
        print("-" * 80)
        
        # Table rows
        for worker_id, data in sorted(self.workers.items()):
            status = "HEALTHY" if data["healthy"] else "DOWN"
            status_color = "\033[92m" if data["healthy"] else "\033[91m"  # Green or Red
            reset_color = "\033[0m"
            
            # Format stats with defaults for missing data
            stats = data.get("stats", {})
            cpu = f"{stats.get('cpu', 0):.1f}" if stats.get('cpu') is not None else "N/A"
            memory_mb = f"{stats.get('memory', 0)/1024/1024:.1f}MB" if stats.get('memory') is not None else "N/A"
            tasks = str(stats.get('active_tasks', "N/A"))
            latency = f"{stats.get('latency', 0):.2f}ms" if stats.get('latency') is not None else "N/A"
            
            # Format last seen time
            if data["last_seen"] > 0:
                last_seen = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data["last_seen"]))
            else:
                last_seen = "Never"
                
            print(f"{worker_id:<25} {status_color}{status:<10}{reset_color} {cpu:<8} {memory_mb:<10} {tasks:<8} {latency:<10} {last_seen:<20}")
            
        print("=" * 80 + "\n")
        
    def get_healthy_workers(self) -> List[str]:
        """Return a list of healthy worker addresses."""
        return [addr for addr, data in self.workers.items() if data["healthy"]]


async def main():
    parser = argparse.ArgumentParser(description='Worker Health Monitor (Master Simulation)')
    parser.add_argument('--master_id', type=str, default='master-1', help='ID for this master instance')
    parser.add_argument('--workers', type=str, required=True, help='Comma-separated list of worker addresses (e.g., localhost:50061,localhost:50062)')
    
    args = parser.parse_args()
    
    # Parse worker addresses
    worker_addresses = [addr.strip() for addr in args.workers.split(',') if addr.strip()]
    
    if not worker_addresses:
        logging.error("No valid worker addresses provided")
        sys.exit(1)
        
    # Create monitor and register workers
    monitor = WorkerHealthMonitor(args.master_id)
    for addr in worker_addresses:
        monitor.register_worker(addr)
        
    try:
        # Start monitoring loop
        logging.info(f"Starting health monitoring for workers: {', '.join(worker_addresses)}")
        await monitor.monitor_workers()
    except KeyboardInterrupt:
        logging.info("Health monitoring interrupted by user")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMonitoring interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True) 