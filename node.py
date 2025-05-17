import asyncio
import grpc
import replication_pb2
import replication_pb2_grpc
import argparse
import logging
import os
import time
import uuid
import tempfile
import subprocess
import glob
from concurrent import futures
from typing import Dict, List, Any, Tuple, Optional, AsyncIterator
import shutil
import sys
import psutil
import ffmpeg
import random

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

SHARDS_DIR = "video_shards"
MASTER_DATA_DIR = "master_data"
MASTER_RETRIEVED_SHARDS_DIR = os.path.join(MASTER_DATA_DIR, "retrieved_shards")
muxer_map = {
    'mp4':  'mp4',
    'mkv':  'matroska',
    'webm': 'webm',
    'mov':  'mov',      
}



STREAM_CHUNK_SIZE = 1024 * 1024

MAX_GRPC_MESSAGE_LENGTH = 1024 * 1024 * 1024  # 1gb

class Node:
    def __init__(self, host: str, port: int, role: str, master_address: Optional[str], known_nodes: List[str]):
        self.host = host
        self.port = port
        self.address = f"{host}:{port}"
        self.role = role
        self.id = str(uuid.uuid4())

        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_address: Optional[str] = None
        self.election_timeout = random.uniform(10, 15)
        self.last_heartbeat_time = time.monotonic()
        self.state = "follower"

        # Store references to background tasks for cancellation
        self._background_tasks: List[asyncio.Task] = []
        self._election_task: Optional[asyncio.Task] = None
        self._pre_election_delay_task: Optional[asyncio.Task] = None
        self._master_announcement_task: Optional[asyncio.Task] = None
        self._other_nodes_health_check_task: Optional[asyncio.Task] = None
        self._master_health_check_task: Optional[asyncio.Task] = None

        self.node_scores = {}  # Store worker scores when acting as master
        self.score_last_updated = 0  # When was score last calculated
        self.score_update_interval = 10  # Seconds between updates
        self.current_score = None  # Will store the latest score data

        self.calculate_server_score(force_fresh=True)
        # Start periodic score update task
        score_update_task = asyncio.create_task(self._update_score_periodically())
        self._background_tasks.append(score_update_task)

        self.video_statuses: Dict[str, Dict[str, Any]] = {}

        self.processing_tasks: Dict[str, asyncio.Task] = {}
        self._unreported_processed_shards: Dict[Tuple[str, str], str] = {}

        self._server: Optional[grpc.aio.Server] = None
        self._channels: Dict[str, grpc.aio.Channel] = {}
        self._node_stubs: Dict[str, replication_pb2_grpc.NodeServiceStub] = {}
        self._worker_stubs: Dict[str, replication_pb2_grpc.WorkerServiceStub] = {}

        self.master_stub: Optional[replication_pb2_grpc.MasterServiceStub] = None
        self._master_channel: Optional[grpc.ServiceChannel] = None
        self._master_channel_address: Optional[str] = None


        self._master_service_added = False
        self._worker_service_added = False

        self.shards_dir = str(self.port) + "-" + SHARDS_DIR
        if self.role == 'master':
            os.makedirs(MASTER_DATA_DIR, exist_ok=True)
            os.makedirs(MASTER_RETRIEVED_SHARDS_DIR, exist_ok=True)
        logging.info(f"Ensured shards directory exists at: {os.path.abspath(self.shards_dir)}")
        logging.info(f"Ensured master data directory exists at: {os.path.abspath(MASTER_DATA_DIR)}")
        logging.info(f"Ensured master retrieved shards directory exists at: {os.path.abspath(MASTER_RETRIEVED_SHARDS_DIR)}")
        self.known_nodes = list(set(known_nodes))
        if self.address in self.known_nodes:
             self.known_nodes.remove(self.address)

        self.current_master_address = master_address

        logging.info(f"[{self.address}] Starting as {self.role.upper()}. Explicit master: {master_address}")

        for node_addr in self.known_nodes:
             if node_addr != self.address:
                self._create_stubs_for_node(node_addr)

        if self.role == 'worker' and self.current_master_address:
            logging.info(f"[{self.address}] Creating/Updating MasterService stubs for {self.current_master_address}")
            self._create_master_stubs(self.current_master_address)
            os.makedirs(self.shards_dir, exist_ok=True)

            # let master know we exist
            asyncio.create_task(self.retry_register_with_master())


        logging.info(f"[{self.address}] Initialized as {self.role.upper()}. Master is {self.current_master_address}. My ID: {self.id}. Current Term: {self.current_term}")

    def _get_or_create_channel(self, node_address: str) -> grpc.aio.Channel:
        """Gets an existing channel or creates a new one."""
        if node_address not in self._channels or (self._channels.get(node_address) and self._channels[node_address]._channel.closed()):
             logging.info(f"[{self.address}] Creating new channel for {node_address} with max message size {MAX_GRPC_MESSAGE_LENGTH} bytes")
             self._channels[node_address] = grpc.aio.insecure_channel(
                 node_address,
                 options=[
                     ('grpc.max_send_message_length', MAX_GRPC_MESSAGE_LENGTH),
                     ('grpc.max_receive_message_length', MAX_GRPC_MESSAGE_LENGTH),
                 ]
             )
        return self._channels[node_address]

    def _create_stubs_for_node(self, node_address: str):
        channel = self._get_or_create_channel(node_address)
        self._node_stubs[node_address] = replication_pb2_grpc.NodeServiceStub(channel)
        if self.role == 'master':
            self._worker_stubs[node_address] = replication_pb2_grpc.WorkerServiceStub(channel)
            logging.info(f"[{self.address}] Created WorkerService stub for {node_address}")


    def _create_master_stubs(self, master_address: str):
        master_channel_valid = self._master_channel and self._master_channel_address == master_address and not self._master_channel._channel.closed()

        if master_channel_valid:
             return

        if self._master_channel:
             asyncio.create_task(self._master_channel.close())
             self._master_channel = None
             self._master_channel_address = None

        self._master_channel = grpc.aio.insecure_channel(
            master_address,
            options=[
                ('grpc.max_send_message_length', MAX_GRPC_MESSAGE_LENGTH),
                ('grpc.max_receive_message_length', MAX_GRPC_MESSAGE_LENGTH),
            ]
        )
        self._master_channel_address = master_address
        self.master_stub = replication_pb2_grpc.MasterServiceStub(self._master_channel)

    def _get_or_create_master_stub(self) -> Optional[replication_pb2_grpc.MasterServiceStub]:
        """Returns the MasterService stub for the current master."""
        current_master_address = self.current_master_address
        if not current_master_address:
             return None
        self._create_master_stubs(current_master_address)
        return self.master_stub
    
    async def _register_with_master(self):
        try:
            req  = replication_pb2.RegisterWorkerRequest(worker_address=self.address)
            resp = await self.master_stub.RegisterWorker(req)
            logging.info(f"[{self.address}] Registered with master: {resp.message}")
        except Exception as e:
            logging.error(f"[{self.address}] Failed to register with master: {e}")

    async def start(self):
        """Starts the gRPC server and background routines."""
        server_options = [
            ('grpc.max_send_message_length', MAX_GRPC_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_GRPC_MESSAGE_LENGTH),
        ]
        self._server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10), options=server_options)

        replication_pb2_grpc.NodeServiceServicer.__init__(self)
        self._server.add_insecure_port(self.address)
        replication_pb2_grpc.add_NodeServiceServicer_to_server(self, self._server)

        # —————————————————————————————————————————————————————————————————
        # Always register both Master + Worker service implementations
        replication_pb2_grpc.MasterServiceServicer.__init__(self)
        replication_pb2_grpc.add_MasterServiceServicer_to_server(self, self._server)
        self._master_service_added = True
        logging.info(f"[{self.address}] MasterServiceServicer added to server.")

        replication_pb2_grpc.WorkerServiceServicer.__init__(self)
        replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
        self._worker_service_added = True
        logging.info(f"[{self.address}] WorkerServiceServicer added to server.")
        # —————————————————————————————————————————————————————————————————

        # Add WorkerService unconditionally if the initial role is worker
        if self.role == 'worker':
            score_reporting_task = asyncio.create_task(self._start_score_reporting())
            self._background_tasks.append(score_reporting_task)
            # replication_pb2_grpc.WorkerServiceServicer.__init__(self)
            # replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
            # self._worker_service_added = True
            # logging.info(f"[{self.address}] WorkerServiceServicer added to server.")


        logging.info(f"[{self.address}] Server starting at {self.address} as {self.role.upper()} with max message size {MAX_GRPC_MESSAGE_LENGTH} bytes")
        await self._server.start()
        logging.info(f"[{self.address}] Server started.")

        logging.info(f"[{self.address}] Performing startup master discovery...")
        discovered_master_address: Optional[str] = None
        highest_term_found = self.current_term

        # Query known nodes for their stats to find potential master
        discovery_tasks = []
        for node_addr in self.known_nodes:
             if node_addr != self.address:
                 node_stub = self._node_stubs.get(node_addr)
                 if node_stub:
                     discovery_tasks.append(asyncio.create_task(self._query_node_for_master(node_stub, node_addr)))

        if discovery_tasks:
             done, pending = await asyncio.wait(discovery_tasks, timeout=5) # Short timeout for startup discovery

             for task in done:
                 try:
                     node_addr, is_master, term = task.result()
                     if is_master and term >= highest_term_found:
                         highest_term_found = term
                         discovered_master_address = node_addr
                         logging.info(f"[{self.address}] Discovered potential master at {node_addr} with term {term}.")
                 except Exception as e:
                     logging.error(f"[{self.address}] Error processing discovery task result: {type(e).__name__} - {e}")

             for task in pending:
                 task.cancel()

        if discovered_master_address and highest_term_found >= self.current_term:
             logging.info(f"[{self.address}] Discovered active master {discovered_master_address} with term {highest_term_found}. Transitioning to follower state.")
             self.state = "follower"
             self.role = 'worker' # Assume node becomes worker if not the elected master
             self.current_term = highest_term_found
             self.voted_for = None
             self.current_master_address = discovered_master_address
             self.leader_address = discovered_master_address
             self.last_heartbeat_time = time.monotonic()

             logging.info(f"[{self.address}] Ensuring master stubs are created for {self.current_master_address} and starting health check routine.")
             self._create_master_stubs(self.current_master_address)
             self._master_health_check_task = asyncio.create_task(self.check_master_health())
             self._background_tasks.append(self._master_health_check_task)


        else:
            logging.info(f"[{self.address}] No active master found with term >= my current term during startup discovery. Proceeding with initial role.")

            # 1) register _both_ gRPC services on every node
            replication_pb2_grpc.add_MasterServiceServicer_to_server(self, self._server)
            logging.info(f"[{self.address}] MasterServiceServicer added to server.")
            replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
            logging.info(f"[{self.address}] WorkerServiceServicer added to server.")

            # 2) now kick off role‐specific background tasks
            if self.role == 'master':
                logging.info(f"[{self.address}] Initializing worker stubs based on known nodes.")
                self._worker_stubs = {}
                for node_addr in self.known_nodes:
                    if node_addr != self.address:
                        self._create_stubs_for_node(node_addr)

                logging.info(f"[{self.address}] Starting master announcement routine.")
                t1 = asyncio.create_task(self._master_election_announcement_routine())
                self._background_tasks.append(t1)
                t2 = asyncio.create_task(self._check_other_nodes_health())
                self._background_tasks.append(t2)

            elif self.role == 'worker':
                logging.info(f"[{self.address}] Starting worker health check routine.")
                t = asyncio.create_task(self.check_master_health())
                self._background_tasks.append(t)

        logging.info(
            f"[{self.address}] Node is now running with state: {self.state}, "
            f"role: {self.role}, current_term: {self.current_term}, "
            f"master: {self.current_master_address}"
        )

        await self._server.wait_for_termination()

    async def _query_node_for_master(self, node_stub: replication_pb2_grpc.NodeServiceStub, node_address: str) -> Tuple[str, bool, int]:
        """Queries a node for its master status and term."""
        try:
            logging.debug(f"[{self.address}] Checking node {node_address} for master status.")
            response = await asyncio.wait_for(
                node_stub.GetNodeStats(replication_pb2.NodeStatsRequest()),
                timeout=2
            )
            logging.debug(f"[{self.address}] Received stats from {node_address}. Is Master: {response.is_master}, Term: {response.current_term}")
            return node_address, response.is_master, response.current_term
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.debug(f"[{self.address}] Node {node_address} unresponsive during startup discovery: {e}")
            return node_address, False, -1 # Indicate not master and invalid term
        except Exception as e:
            logging.error(f"[{self.address}] Unexpected error during startup discovery check for {node_address}: {type(e).__name__} - {e}", exc_info=True)
            return node_address, False, -1


    async def stop(self):
        """Shuts down the gRPC server and cancels background tasks."""
        logging.info(f"[{self.address}] Initiating graceful shutdown.")

        # Cancel all background tasks
        logging.info(f"[{self.address}] Cancelling {len(self._background_tasks)} background tasks.")
        for task in self._background_tasks:
             if not task.done():
                task.cancel()

        # Wait for background tasks to complete their cancellation
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        logging.info(f"[{self.address}] Background tasks cancellation attempted.")

        # Cancel processing tasks (for workers)
        processing_task_list = list(self.processing_tasks.values())
        logging.info(f"[{self.address}] Cancelling {len(processing_task_list)} processing tasks.")
        for task in processing_task_list:
             if not task.done():
                task.cancel()

        await asyncio.gather(*processing_task_list, return_exceptions=True)
        logging.info(f"[{self.address}] Processing tasks cancellation attempted.")

        # Shut down the gRPC server
        if self._server:
             logging.info(f"[{self.address}] Shutting down gRPC server...")
             await self._server.stop(5) # Graceful shutdown with a timeout
             logging.info(f"[{self.address}] gRPC server shut down.")

        # Close all channels
        logging.info(f"[{self.address}] Closing gRPC channels.")
        channel_close_tasks = []
        for address, channel in self._channels.items():
             if channel and not channel._channel.closed():
                 logging.info(f"[{self.address}] Closing channel to {address}")
                 channel_close_tasks.append(asyncio.create_task(channel.close()))

        if self._master_channel and not self._master_channel._channel.closed():
             logging.info(f"[{self.address}] Closing master channel to {self._master_channel_address}")
             channel_close_tasks.append(asyncio.create_task(self._master_channel.close()))

        if channel_close_tasks:
            await asyncio.gather(*channel_close_tasks, return_exceptions=True)
        logging.info(f"[{self.address}] gRPC channels closed.")


        logging.info(f"[{self.address}] Node shutdown complete.")

    async def AnnounceMaster(self, request: replication_pb2.MasterAnnouncement, context: grpc.aio.ServicerContext) -> replication_pb2.MasterAnnouncementResponse:
        """Handles incoming MasterAnnouncement RPCs."""
        logging.info(f"[{self.address}] Received MasterAnnouncement from {context.peer()}. New master is {request.master_address} (ID: {request.node_id_of_master}, Term: {request.term})")

        if request.term > self.current_term:
             logging.info(f"[{self.address}] Received MasterAnnouncement with higher term ({request.term} > {self.current_term}). Updating term and reverting to follower.")
             self.current_term = request.term
             self.state = "follower"
             self.voted_for = None
             self.current_master_address = request.master_address
             self.leader_address = request.master_address # Assuming master is leader in this model
             self.last_heartbeat_time = time.monotonic()
             self.role = 'worker' # A non-master receiving higher term becomes worker

             logging.info(f"[{self.address}] Updating Master stubs to point to {request.master_address} and attempting to report unreported shards.")
             self._create_master_stubs(request.master_address)
             asyncio.create_task(self._attempt_report_unreported_shards())

             # Cancel any pending election/announcement tasks as we are now a follower
             if self._election_task and not self._election_task.done():
                  logging.info(f"[{self.address}] Cancelling pending election task due to new master announcement.")
                  self._election_task.cancel()
                  self._election_task = None
             if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                  logging.info(f"[{self.address}] Cancelling pending pre-election delay due to new master announcement.")
                  self._pre_election_delay_task.cancel()
                  self._pre_election_delay_task = None
             if self._master_announcement_task and not self._master_announcement_task.done():
                  logging.info(f"[{self.address}] Cancelling master announcement task.")
                  self._master_announcement_task.cancel()
                  self._master_announcement_task = None
             if self._other_nodes_health_check_task and not self._other_nodes_health_check_task.done():
                  logging.info(f"[{self.address}] Cancelling other nodes health check task.")
                  self._other_nodes_health_check_task.cancel()
                  self._other_nodes_health_check_task = None

             # Ensure master health check is running if we are a worker
             if self.role == 'worker' and (self._master_health_check_task is None or self._master_health_check_task.done()):
                  logging.info(f"[{self.address}] Starting master health check routine.")
                  self._master_health_check_task = asyncio.create_task(self.check_master_health())
                  self._background_tasks.append(self._master_health_check_task)


        elif request.term == self.current_term:
             if self.state == "follower":
                 logging.info(f"[{self.address}] Received MasterAnnouncement with equal term ({request.term}) while in follower state. Accepting as heartbeat.")
                 self.current_master_address = request.master_address
                 self.leader_address = request.master_address
                 self.last_heartbeat_time = time.monotonic()
                 if self.role == 'worker':
                    logging.info(f"[{self.address}] Updating Master stubs to point to {request.master_address} and attempting to report unreported shards.")
                    self._create_master_stubs(request.master_address)
                    asyncio.create_task(self._attempt_report_unreported_shards())

             elif self.state == "candidate":
                 logging.warning(f"[{self.address}] Received MasterAnnouncement with equal term ({request.term}) while in candidate state. Stepping down to follower.")
                 self.state = "follower"
                 self.voted_for = None
                 self.current_master_address = request.master_address
                 self.leader_address = request.master_address
                 self.last_heartbeat_time = time.monotonic()
                 self.role = 'worker'

                 logging.info(f"[{self.address}] Updating Master stubs to point to {request.master_address} and attempting to report unreported shards.")
                 self._create_master_stubs(request.master_address)
                 asyncio.create_task(self._attempt_report_unreported_shards())

                 if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                      logging.info(f"[{self.address}] Cancelling pending pre-election delay due to stepping down from candidate.")
                      self._pre_election_delay_task.cancel()
                      self._pre_election_delay_task = None
                 if self._election_task and not self._election_task.done():
                      logging.info(f"[{self.address}] Cancelling pending election task.")
                      self._election_task.cancel()
                      self._election_task = None

                 # Ensure master health check is running if we are a worker
                 if self.role == 'worker' and (self._master_health_check_task is None or self._master_health_check_task.done()):
                      logging.info(f"[{self.address}] Starting master health check routine.")
                      self._master_health_check_task = asyncio.create_task(self.check_master_health())
                      self._background_tasks.append(self._master_health_check_task)


             elif self.state == "leader":
                 logging.warning(f"[{self.address}] Received MasterAnnouncement with equal term ({request.term}) while in leader state. Potential split-brain. Ignoring announcement and continuing as leader.")

        elif request.term < self.current_term:
             if self.current_master_address is not None:
                 logging.info(f"[{self.address}] Received MasterAnnouncement with lower term ({request.term} < {self.current_term}). Ignoring.")
             else:
                 logging.warning(f"[{self.address}] Received MasterAnnouncement with lower term ({request.term} < {self.current_term}) but I don't know a master. Accepting for now to discover a master.")
                 self.current_term = request.term
                 self.state = "follower"
                 self.voted_for = None
                 self.current_master_address = request.master_address
                 self.leader_address = request.master_address # Assuming master is leader in this model
                 self.last_heartbeat_time = time.monotonic()
                 self.role = 'worker'

                 logging.info(f"[{self.address}] Updating Master stubs to point to {request.master_address} and attempting to report unreported shards.")
                 self._create_master_stubs(request.master_address)
                 asyncio.create_task(self._attempt_report_unreported_shards())

                 if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                      logging.info(f"[{self.address}] Cancelling pending pre-election delay due to accepting lower term announcement (no master known).")
                      self._pre_election_delay_task.cancel()
                      self._pre_election_delay_task = None

                 # Ensure master health check is running if we are a worker
                 if self.role == 'worker' and (self._master_health_check_task is None or self._master_health_check_task.done()):
                      logging.info(f"[{self.address}] Starting master health check routine.")
                      self._master_health_check_task = asyncio.create_task(self.check_master_health())
                      self._background_tasks.append(self._master_health_check_task)


        response = replication_pb2.MasterAnnouncementResponse(
            status=f"Acknowledged by {self.id}",
            node_id=self.id
        )
        return response

    async def RequestVote(self, request: replication_pb2.VoteRequest, context: grpc.aio.ServicerContext) -> replication_pb2.VoteResponse:
        """Handles incoming VoteRequest RPCs with improved tiebreaking."""
        logging.info(f"[{self.address}] Received VoteRequest from {request.candidate_id} with term {request.term} and score {request.score}")

        # If candidate's term is less than current term, reject
        if request.term < self.current_term:
            logging.info(f"[{self.address}] Rejecting vote: candidate term {request.term} < our term {self.current_term}")
            return replication_pb2.VoteResponse(term=self.current_term, vote_granted=False, voter_id=self.address)
            
        # If candidate's term is greater, update our term and become follower
        if request.term > self.current_term:
            logging.info(f"[{self.address}] Candidate has higher term {request.term} > {self.current_term}, updating term")
            self.current_term = request.term
            self.state = "follower"
            self.voted_for = None
            self.leader_address = None
            self.current_master_address = None
            self.last_heartbeat_time = time.monotonic()  # Reset election timeout
            
        # Vote if we haven't voted yet in this term or already voted for this candidate
        if (self.voted_for is None or self.voted_for == request.candidate_id) and request.term >= self.current_term:
            vote_granted = False

            if not self.score_valid:
                # If somehow we don't have a valid score, calculate once
                self.calculate_server_score()
            my_score = self.current_score["score"]
            # THREE-TIER DECISION LOGIC:
            # 1. Compare scores - lower is better
            if request.score < my_score:
                vote_granted = True
                logging.info(f"[{self.address}] Granting vote: candidate score {request.score} < our score {my_score}")
            # 2. If scores equal, use lexicographical ordering of addresses as tiebreaker
            elif abs(request.score - my_score) < 0.001:  # Small epsilon for float comparison
                if request.candidate_id < self.address:  # Deterministic tiebreaker
                    vote_granted = True
                    logging.info(f"[{self.address}] Granting vote: tied score but candidate ID {request.candidate_id} < our ID {self.address}")
                else:
                    logging.info(f"[{self.address}] Rejecting vote: tied score but candidate ID {request.candidate_id} >= our ID {self.address}")
            else:
                logging.info(f"[{self.address}] Rejecting vote: candidate score {request.score} > our score {my_score}")
            
            if vote_granted:
                self.voted_for = request.candidate_id
                self.last_heartbeat_time = time.monotonic()
        else:
            vote_granted = False
            logging.info(f"[{self.address}] Already voted for {self.voted_for} in term {self.current_term}, rejecting")

        return replication_pb2.VoteResponse(term=self.current_term, vote_granted=vote_granted, voter_id=self.address)

    def reset_election_timer(self):
        """Resets election timer with exponential randomized backoff"""
        base_timeout = 10  # seconds
        max_timeout = 30   # maximum timeout
        
        # Use election attempts to increase backoff with each failed election
        if not hasattr(self, 'election_attempts'):
            self.election_attempts = 1
        else:
            self.election_attempts += 1
        
        # Exponential backoff with randomization
        backoff_factor = min(self.election_attempts, 5)  # Cap at 5 to avoid huge delays
        min_timeout = base_timeout * (1.5 ** backoff_factor)
        max_timeout = min_timeout * 1.5
        
        self.election_timeout = random.uniform(min_timeout, max_timeout)
        logging.info(f"[{self.address}] New election timeout: {self.election_timeout:.2f}s (attempt {self.election_attempts})")
        self.last_heartbeat_time = time.monotonic()


    async def GetNodeStats(self, request: replication_pb2.NodeStatsRequest, context: grpc.aio.ServicerContext) -> replication_pb2.NodeStatsResponse:
        """Provides statistics about the node."""
        logging.debug(f"[{self.address}] Received GetNodeStats request from {context.peer()}")
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_info = psutil.virtual_memory()
        memory_percent = memory_info.percent

        try:
             shards_disk_usage = shutil.disk_usage(self.shards_dir)
             disk_space_free_shards = shards_disk_usage.free
             disk_space_total_shards = shards_disk_usage.total
        except Exception:
             disk_space_free_shards = -1
             disk_space_total_shards = -1

        try:
             master_data_disk_usage = shutil.disk_usage(MASTER_DATA_DIR)
             disk_space_free_masterdata = master_data_disk_usage.free
             disk_space_total_masterdata = master_data_disk_usage.total
        except Exception:
             disk_space_free_masterdata = -1
             disk_space_total_masterdata = -1

        response = replication_pb2.NodeStatsResponse(
            node_id=self.id,
            node_address=self.address,
            is_master=(self.role == 'master'),
            current_master_address=self.current_master_address if self.current_master_address else "",
            cpu_utilization=cpu_percent,
            memory_utilization=memory_percent,
            disk_space_free_shards=disk_space_free_shards,
            disk_space_total_shards=disk_space_total_shards,
            disk_space_free_masterdata=disk_space_free_masterdata,
            disk_space_total_masterdata=disk_space_total_masterdata,
            active_tasks=len(self.processing_tasks) if self.role == 'worker' else len([task for task in asyncio.all_tasks() if task is not asyncio.current_task()]),
            known_nodes_count=len(self.known_nodes) + 1,
            election_in_progress=(self.state in ["candidate", "leader"]),
            current_term=self.current_term
        )
        return response

    async def GetCurrentMaster(self, request: replication_pb2.GetCurrentMasterRequest, context: grpc.aio.ServicerContext) -> replication_pb2.GetCurrentMasterResponse:
        """Provides the address and term of the current master."""
        logging.debug(f"[{self.address}] Received GetCurrentMaster request from {context.peer()}")
        return replication_pb2.GetCurrentMasterResponse(
            master_address=self.current_master_address if self.current_master_address else "",
            term=self.current_term,
            is_master_known=self.current_master_address is not None
        )
    
    def calculate_server_score(self, force_fresh=False):
        """Calculate a score for this node based on system metrics."""
        # Return cached score if it's valid and fresh enough
        if (not force_fresh and self.score_valid and 
                (time.monotonic() - self.score_last_updated) < self.score_update_interval):
            return self.current_score

        # Get system load average (1, 5, 15 minute averages)
        try:
            load_avg = os.getloadavg()[0]  # Use 1-minute average
        except AttributeError:
            load_avg = 0  # Default for Windows which doesn't have getloadavg

        # Get current I/O wait percentage
        cpu_times = psutil.cpu_times_percent()
        io_wait = getattr(cpu_times, "iowait", 0)  # Default to 0 if not available
        
        # Get network usage
        net_io = psutil.net_io_counters()
        net_usage = (net_io.bytes_sent + net_io.bytes_recv) / (1024 * 1024)
        
        # Estimate memory stored (size of video shards directory)
        try:
            memory_stored = sum(
                os.path.getsize(os.path.join(self.shards_dir , f)) 
                for f in os.listdir(self.shards_dir ) 
                if os.path.isfile(os.path.join(self.shards_dir , f))
            ) / (1024 * 1024)
        except:
            memory_stored = 0

        score = (
            (0.3 * min(100, load_avg * 10))
            + (0.2 * io_wait)
            + (0.1 * min(100, net_usage))
            + (0.4 * min(100, memory_stored))
        )

        self.current_score = {
            "server_id": self.address,
            "score": score,
            "load_avg": load_avg,
            "io_wait": io_wait,
            "net_usage_mb": net_usage,
            "memory_stored_mb": memory_stored,
        }
        self.score_valid = True
        self.score_last_updated = time.monotonic()
        
        return self.current_score
    
    async def ReportResourceScore(self, request: replication_pb2.ReportResourceScoreRequest, context: grpc.aio.ServicerContext) -> replication_pb2.ReportResourceScoreResponse:
        """Handles incoming resource scores from workers."""
        if self.role != 'master':
            logging.info(f"[{self.address}] Received score report but I'm not master. Informing worker.")
            return replication_pb2.ReportResourceScoreResponse(
                success=False, 
                message="Not master"
            )
        
        worker_address = request.worker_address
        score = request.resource_score
        
        logging.info(f"[{self.address}] Received resource score from {worker_address}: {score.score}")
        
        # Store score in master's collection
        if not hasattr(self, 'node_scores'):
            self.node_scores = {}
        self.node_scores[worker_address] = score
        
        return replication_pb2.ReportResourceScoreResponse(success=True)

    async def calculate_and_send_score_to_master(self):
        # Skip if no valid master connection
        if (not self.current_master_address or 
            not self._validate_stub(self.current_master_address)):
            logging.debug(f"[{self.address}] No valid master connection for scoring")
            return
        """Calculate local score and send directly to master."""
        # Skip score reporting if no master, I am the master, or election is in progress
        if (not self.current_master_address or 
            self.current_master_address == self.address or
            self._pre_election_delay_task is not None or
            self.state == "candidate"):
            logging.debug(f"[{self.address}] Skipping score report: No master available, I am the master, or election in progress")
            return
        
        # Use stored score (calculate if needed)
        if not self.score_valid:
            self.calculate_server_score()
            
        # Create resource score object
        resource_score = replication_pb2.ResourceScore(
            server_id=self.current_score["server_id"],
            score=self.current_score["score"],
            load_avg=self.current_score["load_avg"],
            io_wait=self.current_score["io_wait"],
            net_usage_mb=self.current_score["net_usage_mb"],
            memory_stored_mb=self.current_score["memory_stored_mb"]
        )
        
        # Get master stub
        master_node_stub = self._node_stubs.get(self.current_master_address)
        if not master_node_stub:
            logging.warning(f"[{self.address}] No stub for master at {self.current_master_address}")
            return
        
        # Send score directly to master
        try:
            request = replication_pb2.ReportResourceScoreRequest(
                worker_address=self.address,
                resource_score=resource_score
            )
            await master_node_stub.ReportResourceScore(request)
            logging.debug(f"[{self.address}] Successfully reported score to master")
        except Exception as e:
            logging.error(f"[{self.address}] Failed to report score to master: {e}")

    async def _update_score_periodically(self):
        """Update score periodically in the background."""
        while not getattr(self, '_shutdown_flag', False):
            # Calculate and store score
            self.calculate_server_score(force_fresh=True)
            logging.debug(f"[{self.address}] Updated score: {self.current_score['score']}")
            await asyncio.sleep(self.score_update_interval)
        
    async def _start_score_reporting(self):
        """Periodically report score to master."""
        while self.role == 'worker' and not getattr(self, '_shutdown_flag', False):
            # Only attempt to report if we're not in an election process
            if self._pre_election_delay_task is None and self.current_master_address:
                await self.calculate_and_send_score_to_master()
            await asyncio.sleep(10)  # Report every 10 seconds

    async def RegisterNode(self, request: replication_pb2.RegisterNodeRequest, context: grpc.aio.ServicerContext) -> replication_pb2.RegisterNodeResponse:
        """Handles registration of new nodes in the network."""
        node_addr = f"{request.address}:{request.port}"
        node_id = request.node_id
        
        logging.info(f"[{self.address}] Received RegisterNode request from {node_id} at {node_addr}")
        
        if node_addr not in self.known_nodes:
            logging.info(f"[{self.address}] Adding new node {node_addr} to known_nodes")
            self.known_nodes.append(node_addr)
            self._create_stubs_for_node(node_addr)
            
            # If we're the master, broadcast updated node list to all nodes
            if self.role == 'master':
                await self.broadcast_node_list()
                
        return replication_pb2.RegisterNodeResponse(
            success=True,
            current_leader=self.current_master_address or "",
            nodes=self.known_nodes
        )

    async def UpdateNodeList(self, request: replication_pb2.UpdateNodeListRequest, context: grpc.aio.ServicerContext) -> replication_pb2.UpdateNodeListResponse:
        """Updates this node's knowledge of the network topology."""
        logging.info(f"[{self.address}] Received UpdateNodeList with {len(request.node_addresses)} nodes")
        
        updated = False
        for node_addr in request.node_addresses:
            if node_addr != self.address and node_addr not in self.known_nodes:
                logging.info(f"[{self.address}] Adding new node {node_addr} to known_nodes")
                self.known_nodes.append(node_addr)
                self._create_stubs_for_node(node_addr)
                updated = True
                
        if request.master_address and request.master_address != self.current_master_address:
            logging.info(f"[{self.address}] Updating master address from {self.current_master_address} to {request.master_address}")
            self.current_master_address = request.master_address
            self.leader_address = request.master_address
            updated = True
            
        return replication_pb2.UpdateNodeListResponse(success=True)

    async def broadcast_node_list(self):
        """Broadcasts the complete node list to all known nodes."""
        if self.role != 'master':
            logging.debug(f"[{self.address}] Not master, skipping node list broadcast")
            return
            
        logging.info(f"[{self.address}] Broadcasting node list to all nodes: {len(self.known_nodes)} nodes")
        
        # Include self in the node list if not already there
        all_nodes = self.known_nodes.copy()
        if self.address not in all_nodes:
            all_nodes.append(self.address)
            
        # Create the request
        request = replication_pb2.UpdateNodeListRequest(
            node_addresses=all_nodes,
            master_address=self.address
        )
        
        # Send to all known nodes
        for node_addr in list(all_nodes):  # Use all_nodes to include self if needed for loops
            if node_addr == self.address:
                continue
                
            node_stub = self._node_stubs.get(node_addr)
            if not node_stub:
                logging.warning(f"[{self.address}] No NodeService stub for {node_addr}, cannot update")
                continue
                
            try:
                await asyncio.wait_for(node_stub.UpdateNodeList(request), timeout=5)
                logging.debug(f"[{self.address}] Successfully sent node list to {node_addr}")
            except Exception as e:
                logging.error(f"[{self.address}] Failed to send node list to {node_addr}: {e}")
        
    async def GetAllNodes(self, request: replication_pb2.GetAllNodesRequest, context: grpc.aio.ServicerContext) -> replication_pb2.GetAllNodesResponse:
        """Returns information about all nodes in the network."""
        logging.info(f"[{self.address}] Received GetAllNodes request")
        
        node_infos = []
        for node_addr in self.known_nodes + [self.address]:
            # Split address into host and port
            if ':' in node_addr:
                host, port_str = node_addr.rsplit(':', 1)
                port = int(port_str)
            else:
                host = node_addr
                port = 0
                
            node_infos.append(
                replication_pb2.NodeInfo(
                    node_id=node_addr,
                    address=host,
                    port=port
                )
            )
            
        return replication_pb2.GetAllNodesResponse(nodes=node_infos)
    
    async def RegisterWorker(self, request: replication_pb2.RegisterWorkerRequest, context: grpc.aio.ServicerContext) -> replication_pb2.RegisterWorkerResponse:
        """RPC called by workers at startup to join the cluster."""
        worker_addr = request.worker_address
        if worker_addr not in self.known_nodes:
            logging.info(f"[{self.address}] RegisterWorker: adding {worker_addr}")
            self.known_nodes.append(worker_addr)
            self._create_stubs_for_node(worker_addr)
            
            # Broadcast updated node list to all nodes
            if self.role == 'master':
                await self.broadcast_node_list()
                
            return replication_pb2.RegisterWorkerResponse(
                success=True,
                message=f"{worker_addr} registered"
            )
        else:
            return replication_pb2.RegisterWorkerResponse(
                success=False,
                message=f"{worker_addr} was already registered"
            )


    
    async def UploadVideo(self, request_iterator: AsyncIterator[replication_pb2.UploadVideoChunk], context: grpc.aio.ServicerContext) -> replication_pb2.UploadVideoResponse:
        if self.role != 'master':
             return replication_pb2.UploadVideoResponse(success=False, message="This node is not the master.")

        video_id: Optional[str] = None
        temp_input_path: Optional[str] = None

        try:
            first_chunk = await anext(request_iterator)
            if not first_chunk.is_first_chunk:
                raise ValueError("First chunk in UploadVideo stream must have is_first_chunk set to True.")

            video_id = first_chunk.video_id or str(uuid.uuid4())
            original_filename = first_chunk.original_filename or f"{video_id}"
            file_extension = original_filename.split('.')[-1] if '.' in original_filename else 'mp4'

            temp_input_path = os.path.join(MASTER_DATA_DIR, f"{video_id}_original.{file_extension}")
            loop = asyncio.get_event_loop()
            with open(temp_input_path, 'wb') as f:
                await loop.run_in_executor(None, f.write, first_chunk.data_chunk)
                async for chunk_message in request_iterator:
                    await loop.run_in_executor(None, f.write, chunk_message.data_chunk)

            self.video_statuses[video_id] = {
                 "status": "segmenting",
                 "target_width": first_chunk.target_width,
                 "target_height": first_chunk.target_height,
                 "original_extension": file_extension,
                 "shards": {},
                 "retrieved_shards": {},
                 "concatenation_task": None
            }

            output_pattern = os.path.join(
                MASTER_DATA_DIR,
                f"{video_id}_shard_%04d.{file_extension}"
            )

            try:
                await loop.run_in_executor(
                    None,
                    lambda: (
                        ffmpeg
                        .input(temp_input_path)
                        .output(
                            output_pattern,
                            format="segment",
                            segment_time=10,
                            reset_timestamps=1,
                            c="copy",
                            acodec="copy",
                            vcodec="copy"
                        )
                        .run(overwrite_output=True)
                    )
                )
                self.video_statuses[video_id]["status"] = "segmented"

                shard_files = sorted(glob.glob(os.path.join(MASTER_DATA_DIR, f"{video_id}_shard_*.{file_extension}")))
                self.video_statuses[video_id]["total_shards"] = len(shard_files)

                distribute_task = asyncio.create_task(
                    self._distribute_shards(
                        video_id, 
                        shard_files,
                        first_chunk.target_width,
                        first_chunk.target_height,
                        original_filename
                    )
                )
                self._background_tasks.append(distribute_task)

                return replication_pb2.UploadVideoResponse(
                    video_id=video_id,
                    success=True,
                    message="Video uploaded and segmented"
                )

            except ffmpeg.Error as e:
                 self.video_statuses[video_id]["status"] = "failed_segmentation"
                 return replication_pb2.UploadVideoResponse(video_id=video_id, success=False, message=f"Segmentation failed: {e.stderr.decode()}")
            except Exception as e:
                 self.video_statuses[video_id]["status"] = "failed_segmentation"
                 return replication_pb2.UploadVideoResponse(video_id=video_id, success=False, message=f"Segmentation failed: {type(e).__name__} - {e}")

        except Exception as e:
             if temp_input_path and os.path.exists(temp_input_path):
                await loop.run_in_executor(None, os.remove, temp_input_path)
             return replication_pb2.UploadVideoResponse(
                 video_id=video_id if video_id else "unknown",
                 success=False,
                 message=f"Upload failed: {type(e).__name__} - {e}"
             )
        
    async def _distribute_shards(self, video_id: str, shard_files: List[str], target_width: int, target_height: int, original_filename: str):
        """Distributes all shards to workers immediately using parallel tasks"""
        logging.info(f"[{self.address}] Starting bulk distribution of {len(shard_files)} shards")

        max_retries = 5
        retry_delay = 5  # seconds
        valid_workers = []
        for attempt in range(max_retries):
            # Check for valid workers (non-None stubs)
            valid_workers = [addr for addr in self._worker_stubs if self._worker_stubs.get(addr) is not None]
            if valid_workers:
                break
            logging.warning(f"[{self.address}] No workers available. Retrying in {retry_delay}s (attempt {attempt+1}/{max_retries})")
            await asyncio.sleep(retry_delay)
        if not valid_workers:
            logging.error(f"[{self.address}] Aborting distribution: No workers after {max_retries} retries")
            return

        # Create distribution tasks only for valid workers
        distribution_tasks = []
        for idx, shard_file in enumerate(shard_files):
            worker = valid_workers[idx % len(valid_workers)]
            distribution_tasks.append(
                self._send_shard_to_worker(
                    worker=worker,
                    video_id=video_id,
                    shard_file=shard_file,
                    shard_index=idx,
                    total_shards=len(shard_files),
                    target_width=target_width,
                    target_height=target_height,
                    original_filename=original_filename
                )
            )

        # Run all distribution tasks in parallel
        results = await asyncio.gather(*distribution_tasks, return_exceptions=True)
        
        # Track failures
        successful = sum(1 for r in results if r is True)
        failed = len(shard_files) - successful
        
        if failed == 0:
            self._update_video_status(video_id, "distributed", f"All {len(shard_files)} shards distributed")
        else:
            self._update_video_status(video_id, "partial_distribution", 
                                    f"{successful} succeeded, {failed} failed")

    async def _send_shard_to_worker(self, worker: str, video_id: str, shard_file: str,
                                shard_index: int, total_shards: int, target_width: int,
                                target_height: int, original_filename: str):
        """Async helper to send a single shard to a worker"""
        try:
            worker_stub = self._worker_stubs.get(worker)
            if not worker_stub:
                logging.error(f"[{self.address}] Worker stub for {worker} is missing or invalid. Cannot distribute {shard_file}.")
                # Consider adding status update for this shard here if needed
                return False

            # Corrected: Get channel from self._channels using the worker address
            channel = self._channels.get(worker)
            if not channel:
                 logging.error(f"[{self.address}] Channel for worker {worker} not found in _channels. Cannot distribute {shard_file}.")
                 # Consider adding status update for this shard here if needed
                 return False

            # Check channel state before attempting RPC
            if channel.get_state() == grpc.ChannelConnectivity.SHUTDOWN:
                logging.error(f"[{self.address}] Channel to worker {worker} is shut down. Cannot distribute {shard_file}.")
                 # Consider adding status update for this shard here if needed
                return False


            shard_data = await asyncio.get_event_loop().run_in_executor(
                None, self._read_file_blocking, shard_file
            )

            request = replication_pb2.DistributeShardRequest(
                video_id=video_id,
                shard_id=os.path.basename(shard_file),
                shard_data=shard_data,
                shard_index=shard_index,
                total_shards=total_shards,
                target_width=target_width,
                target_height=target_height,
                original_filename=original_filename
            )

            logging.info(f"[{self.address}] Attempting to send shard {os.path.basename(shard_file)} to {worker}")
            await asyncio.wait_for(
                worker_stub.ProcessShard(request),
                timeout=30000
            )
            logging.info(f"[{self.address}] Successfully sent shard {os.path.basename(shard_file)} to {worker}")


            # Only remove the local file after successful sending
            await asyncio.get_event_loop().run_in_executor(
                None, self._remove_file_blocking, shard_file
            )
            return True
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.error(f"[{self.address}] RPC failed or timed out when distributing {shard_file} to {worker}: {type(e).__name__} - {e.details() if isinstance(e, grpc.aio.AioRpcError) else e}. Marking shard as failed distribution.", exc_info=True)
            return False
        except Exception as e:
            logging.error(f"[{self.address}] Failed to distribute {shard_file} to {worker}: {type(e).__name__} - {e}. Marking shard as failed distribution.", exc_info=True)
            return False

    # Helper functions for blocking file operations to be used with run_in_executor
    def _read_file_blocking(self, filepath):
        with open(filepath, 'rb') as f:
            return f.read()

    def _remove_file_blocking(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

    async def ReportWorkerShardStatus(self, request: replication_pb2.ReportWorkerShardStatusRequest, context: grpc.aio.ServicerContext) -> replication_pb2.ReportWorkerShardStatusResponse:
        """Handles status updates from worker nodes (master only)."""
        if self.role != 'master':
            return replication_pb2.ReportWorkerShardStatusResponse(success=False, message="This node is not the master.")

        video_id = request.video_id
        shard_id = request.shard_id
        worker_address = request.worker_address
        status = request.status

        logging.info(f"[{self.address}] Received ReportWorkerShardStatus for video {video_id}, shard {shard_id} from {worker_address} with status: {status}")

        if video_id not in self.video_statuses:
            logging.warning(f"[{self.address}] Received status update for unknown video ID: {video_id}")
            return replication_pb2.ReportWorkerShardStatusResponse(success=False, message=f"Unknown video ID: {video_id}")

        if shard_id in self.video_statuses[video_id]["shards"] and self.video_statuses[video_id]["shards"][shard_id]["status"] in ["failed_sending", "rpc_failed", "failed_distribution"]:
            logging.info(f"[{self.address}] Received status for shard {shard_id} previously marked as failed distribution. Updating status.")
            original_index = self.video_statuses[video_id]["shards"][shard_id].get("index", -1)
            self.video_statuses[video_id]["shards"][shard_id] = {
                "status": status,
                "worker_address": worker_address,
                "index": original_index
            }
        elif shard_id not in self.video_statuses[video_id]["shards"]:
            logging.warning(f"[{self.address}] Received status update for unknown shard ID {shard_id} for video {video_id} that wasn't in the initial distribution list.")
            self.video_statuses[video_id]["shards"][shard_id] = {
                "status": status,
                "worker_address": worker_address,
                "index": -1
            }
        else:
            self.video_statuses[video_id]["shards"][shard_id]["status"] = status
            self.video_statuses[video_id]["shards"][shard_id]["worker_address"] = worker_address

        if status == "processed_successfully":
            # Retrieve processed shard as a background task
            retrieve_task = asyncio.create_task(self._retrieve_processed_shard(video_id, shard_id, worker_address))
            self._background_tasks.append(retrieve_task)

        return replication_pb2.ReportWorkerShardStatusResponse(success=True, message="Status updated.")


    async def _retrieve_processed_shard(self, video_id: str, shard_id: str, worker_address: str):
        """Retrieves a processed shard from a worker node."""
        logging.info(f"[{self.address}] Requesting processed shard {shard_id} for video {video_id} from worker {worker_address}")

        worker_stub = self._worker_stubs.get(worker_address)

        
        if not worker_stub:
            logging.error(f"[{self.address}] No WorkerService stub for {worker_address}. Cannot retrieve shard {shard_id}. Marking shard as retrieval failed.")
            if shard_id in self.video_statuses[video_id]["shards"]:
                self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_failed"
                self.video_statuses[video_id]["shards"][shard_id]["message"] = "No worker stub available for retrieval."
            return

        try:
            request = replication_pb2.RequestShardRequest(shard_id=shard_id)
            response = await asyncio.wait_for(worker_stub.RequestShard(request), timeout=30)

            if response.success:
                logging.info(f"[{self.address}] Successfully retrieved processed shard {shard_id} from {worker_address}")
                if video_id in self.video_statuses and shard_id in self.video_statuses[video_id]["shards"]:
                    self.video_statuses[video_id]["retrieved_shards"][shard_id] = {
                        "data": response.shard_data,
                        "index": self.video_statuses[video_id]["shards"][shard_id].get("index", -1)
                    }
                    self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieved"

                    video_info = self.video_statuses[video_id]
                    total_successfully_processed_shards = sum(
                        1 for s in video_info["shards"].values() if s["status"] in ["processed_successfully", "retrieved"]
                    )
                    retrieved_count = sum(
                        1 for s in video_info["shards"].values() if s["status"] == "retrieved"
                    )

                    logging.info(f"[{self.address}] Video {video_id} — retrieved {retrieved_count}/{total_successfully_processed_shards} processed shards.")
                    video_info = self.video_statuses[video_id]
                    total_shards = video_info["total_shards"]
                    retrieved_count = len(video_info["retrieved_shards"])
                    # ✅ If all processed shards retrieved → set status + concat
                    if retrieved_count == total_shards:
                        if video_info["concatenation_task"] is None or video_info["concatenation_task"].done():
                            logging.info(f"All {total_shards} shards retrieved. Starting concatenation.")
                            video_info["status"] = "concatenating"
                            concat_task = asyncio.create_task(self._concatenate_shards(video_id))
                            self._background_tasks.append(concat_task)

                else:
                    logging.warning(f"[{self.address}] Received processed shard {shard_id} for video {video_id} but video/shard info not found in status tracking. Dropping shard data.")

            else:
                logging.error(f"[{self.address}] Worker {worker_address} failed to provide shard {shard_id}: {response.message}. Marking shard as retrieval failed.")
                if shard_id in self.video_statuses[video_id]["shards"]:
                    self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_failed"
                    self.video_statuses[video_id]["shards"][shard_id]["message"] = response.message

        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.error(f"[{self.address}] RPC failed or timed out when retrieving shard {shard_id} from {worker_address}: {e.code()} - {e.details()}. Marking shard as retrieval failed.", exc_info=True)
            if shard_id in self.video_statuses[video_id]["shards"]:
                self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_rpc_failed"
                self.video_statuses[video_id]["shards"][shard_id]["message"] = f"RPC failed or timed out: {type(e).__name__} - {e}"
        except Exception as e:
            logging.error(f"[{self.address}] Failed to retrieve shard {shard_id} from {worker_address}: {type(e).__name__} - {e}", exc_info=True)
            if shard_id in self.video_statuses[video_id]["shards"]:
                self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_failed"
                self.video_statuses[video_id]["shards"][shard_id]["message"] = f"Retrieval failed: {type(e).__name__} - {e}"



    async def _concatenate_shards(self, video_id: str):
        """Concatenates all retrieved shards into the final processed video."""
        logging.info(f"[{self.address}] Starting concatenation for video {video_id}")

        if video_id not in self.video_statuses:
            logging.error(f"Cannot concatenate shards. Video ID {video_id} not found.")
            return

        video_info = self.video_statuses[video_id]
        shards = video_info["retrieved_shards"]
        container = video_info.get("container", "mp4")  # Ensure container is stored during upload

        # Sort shards by index
        sorted_shards = sorted(shards.items(), key=lambda item: item[1]["index"])

        tmp_dir = tempfile.mkdtemp(prefix=f"concat_{video_id}_")
        file_list_path = os.path.join(tmp_dir, "file_list.txt")
        output_path = os.path.join(MASTER_DATA_DIR, f"{video_id}_processed.{container}")

        try:
            # Write shard data to temp files and create file list
            with open(file_list_path, 'w') as f:
                for shard_id, shard_data in sorted_shards:
                    shard_filename = os.path.join(tmp_dir, shard_id)
                    with open(shard_filename, 'wb') as shard_file:
                        shard_file.write(shard_data["data"])
                    f.write(f"file '{shard_filename}'\n")

            # Validate all shard files exist
            for shard_id, _ in sorted_shards:
                if not os.path.exists(os.path.join(tmp_dir, shard_id)):
                    raise FileNotFoundError(f"Shard {shard_id} missing in temp dir.")

            # Build FFmpeg command with error handling
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",
                "-f", "concat",
                "-safe", "0",
                "-copytb", "1",
                "-i", file_list_path,
                "-c", "copy",
                output_path
            ]

            logging.info(f"[{self.address}] Running FFmpeg: {' '.join(ffmpeg_cmd)}")
            result = subprocess.run(
                ffmpeg_cmd,
                capture_output=True,
                text=True,
                check=True
            )

            logging.info(f"[{self.address}] Concatenation succeeded: {output_path}")
            video_info["status"] = "completed"
            video_info["processed_video_path"] = output_path

        except subprocess.CalledProcessError as e:
            logging.error(f"[{self.address}] FFmpeg failed (code {e.returncode}): {e.stderr}")
            video_info["status"] = "concatenation_failed"
            video_info["message"] = f"FFmpeg error: {e.stderr}"
        except Exception as e:
            logging.error(f"[{self.address}] Concatenation error: {e}")
            video_info["status"] = "concatenation_failed"
            video_info["message"] = str(e)
        finally:
            shutil.rmtree(tmp_dir)
            logging.info(f"[{self.address}] Cleaned up temp dir: {tmp_dir}")


    # Helper functions for blocking file operations to be used with run_in_executor
    def _write_file_blocking(self, filepath, data):
        with open(filepath, 'wb') as f:
            f.write(data)

    async def RetrieveVideo(self, request: replication_pb2.RetrieveVideoRequest, context: grpc.aio.ServicerContext) -> AsyncIterator[replication_pb2.RetrieveVideoChunk]:
        """Handles video retrieval requests (master only)."""
        if self.role != 'master':
             logging.error(f"[{self.address}] RetrieveVideo request received by non-master node.")
             await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "This node is not the master.")
             return

        video_id = request.video_id
        logging.info(f"[{self.address}] Received RetrieveVideo request for video ID: {video_id}")

        video_info = self.video_statuses.get(video_id)
        if not video_info:
             logging.error(f"[{self.address}] Video not found for retrieval: {video_id}")
             await context.abort(grpc.StatusCode.NOT_FOUND, "Video not found.")
             return

        if video_info["status"] != "completed":
             status_message = f"Video processing status: {video_info['status']}. Not yet completed."
             logging.error(f"[{self.address}] Video not completed for retrieval: {video_id}. Status: {video_info['status']}")
             await context.abort(grpc.StatusCode.FAILED_PRECONDITION, status_message)
             return

        processed_video_path = video_info.get("processed_video_path")
        if not processed_video_path or not os.path.exists(processed_video_path):
             logging.error(f"[{self.address}] Processed video file not found for {video_id} at {processed_video_path}")
             await context.abort(grpc.StatusCode.INTERNAL, "Processed video file not found on master.")
             return

        loop = asyncio.get_event_loop() # Get the event loop for run_in_executor

        try:
             logging.info(f"[{self.address}] Streaming processed video file {processed_video_path} for video ID: {video_id}")
             # Use run_in_executor for blocking file read
             with open(processed_video_path, 'rb') as f:
                while True:
                    chunk = await loop.run_in_executor(None, f.read, STREAM_CHUNK_SIZE)
                    if not chunk:
                        break
                    yield replication_pb2.RetrieveVideoChunk(video_id=video_id, data_chunk=chunk)

             logging.info(f"[{self.address}] Finished streaming processed video for video ID: {video_id}")

        except Exception as e:
             logging.error(f"[{self.address}] Failed to stream processed video file for {video_id}: {type(e).__name__} - {e}", exc_info=True)
             await context.abort(grpc.StatusCode.INTERNAL, f"Failed to stream processed video file: {type(e).__name__} - {e}")

    async def GetVideoStatus(self, request: replication_pb2.VideoStatusRequest, context: grpc.aio.ServicerContext) -> replication_pb2.VideoStatusResponse:
        """Provides the processing status of a video (master only)."""
        if self.role != 'master':
             return replication_pb2.VideoStatusResponse(video_id=request.video_id, status="not_master", message="This node is not the master and does not track video status.")

        video_id = request.video_id
        logging.debug(f"[{self.address}] Received GetVideoStatus request for video ID: {video_id}")

        video_info = self.video_statuses.get(video_id)
        if not video_info:
             return replication_pb2.VideoStatusResponse(video_id=video_id, status="not_found", message="Video not found.")

        status = video_info.get("status", "unknown")
        message = video_info.get("message", "")

        if status in ["segmented", "shards_distributed", "all_shards_retrieved", "processing_failed", "concatenation_failed", "concatenation_prerequisites_failed"]:
             total_shards = video_info.get("total_shards", 0)
             processed_count = sum(1 for s in video_info["shards"].values() if s["status"] == "processed_successfully" or s["status"] == "retrieved")
             retrieved_count = sum(1 for s in video_info["shards"].values() if s["status"] == "retrieved")
             failed_count = sum(1 for s in video_info["shards"].values() if s["status"] in ["failed_processing", "rpc_failed", "failed_sending", "retrieval_failed", "retrieval_rpc_failed", "failed_distribution"])
             message = f"Status: {status}. Total shards: {total_shards}. Successfully processed/retrieved: {processed_count}. Retrieved by master: {retrieved_count}. Failed: {failed_count}. Details: {message}"

        return replication_pb2.VideoStatusResponse(video_id=video_id, status=status, message=message)

    async def ProcessShard(self, request: replication_pb2.DistributeShardRequest, context: grpc.aio.ServicerContext):
        # Immediately acknowledge receipt and process in background
        asyncio.create_task(self._process_shard_background(request))
        return replication_pb2.ProcessShardResponse(
            shard_id=request.shard_id,
            success=True,
            message="Shard accepted for processing"
        )

    async def _process_shard_background(self, request):
        """Actual processing in background with parallel capacity"""
        process_dir = None # Initialize process_dir to None
        try:
            # Create unique working directory
            process_dir = tempfile.mkdtemp(prefix=f"process_{request.shard_id}_")
            # os.makedirs(process_dir, exist_ok=True) # mkdtemp already creates the directory

            # Write input file
            input_path = os.path.join(process_dir, "input")
            await asyncio.get_event_loop().run_in_executor(
                None, self._write_file_blocking, input_path, request.shard_data
            )

            # Determine the container (extension) from the shard_id
            container = request.shard_id.split(".")[-1] if "." in request.shard_id else "mp4"
            processed_output_filename = f"{request.shard_id}_processed.{container}"
            processed_output_temp_path = os.path.join(process_dir, processed_output_filename)
            processed_output_final_path = os.path.join(self.shards_dir, processed_output_filename)


            # Process with FFmpeg in thread pool
            await asyncio.get_event_loop().run_in_executor(
                None,  # Uses default executor (thread pool)
                self._run_ffmpeg_processing,
                input_path,
                request.target_width,
                request.target_height,
                processed_output_temp_path # Pass the temp output path to FFmpeg
            )

            # Move the processed file to the designated shards directory
            await asyncio.get_event_loop().run_in_executor(
                None, shutil.move, processed_output_temp_path, processed_output_final_path
            )


            await self._report_shard_status(
                request.video_id,
                request.shard_id,
                "processed_successfully",
                "Processing completed and file saved." # Provide a status message, not data
            )

        except Exception as e:
            logging.error(f"[{self.address}] Error processing shard {request.shard_id}: {type(e).__name__} - {e}", exc_info=True)
            await self._report_shard_status(
                request.video_id,
                request.shard_id,
                "failed_processing",
                f"Processing failed: {type(e).__name__} - {e}"
            )
        finally:
            # Clean up the temporary directory
            if process_dir and os.path.exists(process_dir):
                 await asyncio.get_event_loop().run_in_executor(None, shutil.rmtree, process_dir, ignore_errors=True)


    def _run_ffmpeg_processing(self, input_path, target_w, target_h, output_path):
        """Synchronous FFmpeg processing in thread pool"""
        # output_path is now passed in, no need to construct it here
        (
            ffmpeg
            .input(input_path)
            .filter('scale', w=target_w, h=target_h)
            .output(output_path, vcodec='libx264', preset='fast', acodec='copy', loglevel='info') # Added acodec copy and loglevel
            .run(overwrite_output=True, capture_stdout=True, capture_stderr=True) # Capture output for debugging
        )

    async def RequestShard(self, request: replication_pb2.RequestShardRequest, context: grpc.aio.ServicerContext) -> replication_pb2.RequestShardResponse:
        shard_id = request.shard_id  # e.g. “videoid_shard_0002.mkv”
        # Extract container (extension) from shard_id
        container = shard_id.split(".")[-1] if "." in shard_id else "mp4"
        processed_fn = f"{shard_id}_processed.{container}"
        processed_path = os.path.join(self.shards_dir , processed_fn)

        logging.info(f"[{self.address}] RequestShard for {shard_id}, looking at {processed_fn}")

        if not os.path.exists(processed_path):
            msg = f"Processed shard file not found. Expected at {processed_path}" # Improved error message
            logging.error(f"[{self.address}] {msg}")
            return replication_pb2.RequestShardResponse(
                shard_id=shard_id, success=False, message=msg
            )

        # Read the correctly-named file
        try:
            # Use run_in_executor for blocking file read
            data = await asyncio.get_event_loop().run_in_executor(None, self._read_file_blocking, processed_path)

            # Optionally clean it up - moved inside try block
            # await asyncio.get_event_loop().run_in_executor(None, os.remove, processed_path)
            # logging.info(f"[{self.address}] Cleaned up processed shard file: {processed_fn}")

            return replication_pb2.RequestShardResponse(
                shard_id=shard_id,
                success=True,
                shard_data=data,
                message="OK"
            )
        except Exception as e:
             msg = f"Failed to read or clean up processed shard file {processed_fn}: {type(e).__name__} - {e}"
             logging.error(f"[{self.address}] {msg}", exc_info=True)
             return replication_pb2.RequestShardResponse(
                shard_id=shard_id, success=False, message=msg
            )

   


    def _all_shards_processed_successfully(self, video_info):
        """True if all shards were processed successfully or retrieved."""
        statuses = [s["status"] for s in video_info["shards"].values()]
        return all(status in ["processed_successfully", "retrieved"] for status in statuses)

    def _all_shards_retrieved(self, video_info):
        """True if all shards have been retrieved."""
        statuses = [s["status"] for s in video_info["shards"].values()]
        return all(status == "retrieved" for status in statuses)

    def _any_shard_failed(self, video_info):
        """True if any shard failed processing or retrieval."""
        statuses = [s["status"] for s in video_info["shards"].values()]
        return any(status.startswith("failed") or status.endswith("retrieval_failed") for status in statuses)

    async def _report_shard_status(self, video_id: str, shard_id: str, status: str, message: str = ""):
        """Reports the processing status of a shard to the master (worker only)."""
        master_stub = self._get_or_create_master_stub()
        if not master_stub:
             logging.error(f"[{self.address}] Cannot report shard status for {shard_id}. No master MasterService stub available. Storing as unreported.")
             self._unreported_processed_shards[(video_id, shard_id)] = status
             return

        request = replication_pb2.ReportWorkerShardStatusRequest(
            video_id=video_id,
            shard_id=shard_id,
            worker_address=self.address,
            status=status
        )

        try:
             logging.info(f"[{self.address}] Attempting to report status '{status}' for shard {shard_id} of video {video_id} to master {self.current_master_address} via MasterService stub")
             response = await master_stub.ReportWorkerShardStatus(request)
             if response.success:
                logging.info(f"[{self.address}] Successfully reported status for shard {shard_id}.")
                if (video_id, shard_id) in self._unreported_processed_shards:
                    del self._unreported_processed_shards[(video_id, shard_id)]
                    logging.info(f"[{self.address}] Removed shard {shard_id} from unreported list after successful report.")
             else:
                logging.error(f"[{self.address}] Master rejected shard status report for {shard_id}: {response.message}. Storing as unreported.")
                self._unreported_processed_shards[(video_id, shard_id)] = status

        except grpc.aio.AioRpcError as e:
             logging.error(f"[{self.address}] RPC failed when reporting shard status for {shard_id} to master {self.current_master_address}: {e.code()} - {e.details()}. Storing as unreported.", exc_info=True)
             self._unreported_processed_shards[(video_id, shard_id)] = status
        except Exception as e:
             logging.error(f"[{self.address}] Failed to report shard status for {shard_id} to master {self.current_master_address}: {type(e).__name__} - {e}. Storing as unreported.", exc_info=True)
             self._unreported_processed_shards[(video_id, shard_id)] = status

    async def _attempt_report_unreported_shards(self):
        """Attempts to report processed shards that failed to report earlier (worker only)."""
        if not self._unreported_processed_shards:
            logging.debug(f"[{self.address}] No unreported processed shards to report.")
            return

        logging.info(f"[{self.address}] Attempting to report {len(self._unreported_processed_shards)} unreported processed shards to the new master {self.current_master_address}.")

        shards_to_report = list(self._unreported_processed_shards.items())
        for (video_id, shard_id), status in shards_to_report:
            # Call _report_shard_status which will use the current master stub
            await self._report_shard_status(video_id, shard_id, status)
            # _report_shard_status will remove the shard from _unreported_processed_shards if successful

    
    
    async def _check_other_nodes_health(self):
        """
        Periodically checks every known node:
        - If we don’t yet have a stub for it, try to create one.
        - Otherwise, call GetNodeStats() as a lightweight health‐check.
        Works whether master or workers come up in any order.
        """
        # Only masters run this loop
        if self.role != 'master':
            logging.debug(f"[{self.address}] Not master, skipping health checks")
            return

        HEALTH_INTERVAL = 5.0      # seconds between full sweeps
        JITTER        = 2.0        # up to this much random extra delay
        TIMEOUT       = 3.0        # seconds per RPC

        logging.info(f"[{self.address}] Starting other‐nodes health check routine")

        while True:
            # bail out if demoted
            if self.role != 'master':
                logging.info(f"[{self.address}] No longer master, stopping health checks")
                break

            for node_addr in list(self.known_nodes):
                if node_addr == self.address:
                    continue

                # 1) If we never stubbed this node, try to wire it up now
                if node_addr not in self._node_stubs:
                    try:
                        logging.info(f"[{self.address}] Discovered new node {node_addr}, creating stubs")
                        self._create_stubs_for_node(node_addr)
                        logging.info(f"[{self.address}] Stubs successfully created for {node_addr}")
                    except Exception as e:
                        logging.debug(f"[{self.address}] Still can’t reach {node_addr}: {e}")
                    # move on whether it succeeded or not
                    continue

                # 2) We have a stub—do a real health check
                stub = self._node_stubs[node_addr]
                try:
                    await asyncio.wait_for(
                        stub.GetNodeStats(replication_pb2.NodeStatsRequest()),
                        timeout=TIMEOUT
                    )
                    logging.debug(f"[{self.address}] Node {node_addr} is healthy")
                except (grpc.aio.AioRpcError, asyncio.TimeoutError) as rpc_err:
                    logging.warning(f"[{self.address}] Health check failed for {node_addr}: {rpc_err}")
                    # Prune this node out of our cluster
                    if node_addr in self.known_nodes:
                        logging.info(f"[{self.address}] Removing unreachable node {node_addr} from known_nodes")
                        self.known_nodes.remove(node_addr)
                    # Tear down its stubs so we stop announcing to it
                    self._node_stubs.pop(node_addr, None)
                    self._worker_stubs.pop(node_addr, None)
                    # *** NEW: also close and drop its channel ***
                    if node_addr in self._channels:
                        ch = self._channels.pop(node_addr)
                        try:
                            asyncio.create_task(ch.close())
                            logging.info(f"[{self.address}] Closed channel to {node_addr}")
                        except Exception:
                            pass

                except Exception as exc:
                    logging.error(f"[{self.address}] Unexpected error checking {node_addr}: {exc}", exc_info=True)

            # pause a bit (with jitter) before next sweep
            await asyncio.sleep(HEALTH_INTERVAL + random.uniform(0, JITTER))

    async def start_election(self):
        """Initiates leader election with improved coordination"""
        if self.state == "leader":
            return

        # Before becoming a candidate, verify if we should run at all
        # my_score = self.calculate_server_score()["score"]
        
        # Check if there are better-scoring nodes that should be leaders instead
        better_nodes = []
        for node_addr in self.known_nodes:
            if node_addr == self.address:
                continue
                
            # Skip checking nodes we know are unreachable
            if not self._validate_stub(node_addr):
                continue
                
            try:
                # Query node's score
                node_stub = self._node_stubs.get(node_addr)
                if node_stub:
                    response = await asyncio.wait_for(node_stub.GetNodeStats(replication_pb2.NodeStatsRequest()), timeout=2)
                    node_score = response.cpu_utilization  # assuming this approximates the score
                    if node_score < my_score:
                        better_nodes.append((node_addr, node_score))
            except Exception:
                continue
        
        # If better candidates exist, wait longer before starting election
        if better_nodes:
            logging.info(f"[{self.address}] Found {len(better_nodes)} better-scoring nodes, delaying election")
            await asyncio.sleep(random.uniform(8, 12))  # Wait for better nodes to start election
            
            # Check if another election is already in progress
            if self.state != "follower" or self._pre_election_delay_task is not None:
                logging.info(f"[{self.address}] Election already started by another node, aborting")
                return
        
        # Now proceed with election
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.address
        self.votes_received = 1  # Vote for self
        
        # Record failed master before clearing it
        failed_master = self.current_master_address
        self.leader_address = None
        self.current_master_address = None

        logging.info(f"[{self.address}] Starting election for term {self.current_term}")

        # Calculate our score
        score_data = self.calculate_server_score()

        # Create vote request with score
        request = replication_pb2.VoteRequest(
            term=self.current_term,
            candidate_id=self.address,
            score=score_data["score"],
            memory_stored_mb=score_data["memory_stored_mb"]
        )

        # Get list of alive nodes (excluding recently failed master)
        alive_nodes = [addr for addr in self.known_nodes 
                    if addr != failed_master and addr != self.address]

        # Request votes from all known nodes
        vote_tasks = []
        for node_addr in alive_nodes:
            if node_addr == failed_master:
                logging.info(f"[{self.address}] Skipping vote request to known failed node {node_addr}")
                continue
            if node_addr != self.address:
                # Skip nodes that we know are down
                if node_addr == self.current_master_address and self.current_master_address is None:
                    logging.info(f"[{self.address}] Skipping vote request to known failed node {node_addr}")
                    continue
                    
                node_stub = self._node_stubs.get(node_addr)
                if node_stub:
                    try:
                        logging.info(f"[{self.address}] Sending vote request to {node_addr}")
                        task = asyncio.create_task(
                            self._send_request_vote(node_stub, request, node_addr)
                        )
                        vote_tasks.append(task)
                    except Exception as e:
                        logging.error(f"[{self.address}] Error creating vote request for {node_addr}: {e}")

        if not vote_tasks:
            logging.info(f"[{self.address}] No other nodes to request votes from, becoming leader")
            await self._become_master()
            return

        # Wait for vote responses
        results = await asyncio.gather(*vote_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"[{self.address}] Exception during vote request: {result}")
                continue
                
            if self.state != "candidate":  # If we're no longer a candidate, stop processing votes
                return
                
            if result.term > self.current_term:
                self.current_term = result.term
                self.state = "follower"
                self.voted_for = None
                self.last_heartbeat_time = time.monotonic()
                return
                
            if result.vote_granted:
                self.votes_received += 1
                logging.info(f"[{self.address}] Received vote from {result.voter_id}, total: {self.votes_received}")

        # Check if we have majority
        total_nodes = len(self.known_nodes) + 1  # +1 for self
        if self.votes_received > total_nodes / 2:
            logging.info(f"[{self.address}] Won election with {self.votes_received} votes out of {total_nodes}")
            await self._become_master()
        else:
            # Reset to follower state
            self.state = "follower"
            if self._master_health_check_task is None or self._master_health_check_task.done():
                logging.info(f"[{self.address}] Restarting master health check after failed election")
                self._master_health_check_task = asyncio.create_task(self.check_master_health())
                self._background_tasks.append(self._master_health_check_task)

    def _validate_stub(self, node_addr: str) -> bool:
        """Returns True if stub is valid and connected"""
        if node_addr not in self._node_stubs:
            return False
        try:
            # Check channel state
            channel = self._node_stubs[node_addr].channel
            return channel.get_state(try_to_connect=True) == grpc.ChannelConnectivity.READY
        except Exception:
            return False

    def get_best_nodes_by_score(self, count=1):
        """Returns the best nodes based on their scores."""
        if not hasattr(self, 'node_scores') or not self.node_scores:
            # Fallback to self if no scores available
            return [self.address]
        
        # Sort nodes by score (lower is better)
        sorted_nodes = sorted(self.node_scores.keys(), 
                            key=lambda addr: self.node_scores[addr].score)
        
        # Return the best 'count' nodes
        return sorted_nodes[:count]

    async def _become_master(self):
        """Transitions the node to the master role."""
        if hasattr(self, 'node_scores') and self.node_scores:
            # Get the best node by score
            best_node = self.get_best_nodes_by_score(1)[0]
            
            # If we're not the best, defer to the better node
            if best_node != self.address and best_node in self.known_nodes:
                logging.info(f"[{self.address}] Found better master candidate: {best_node}. Deferring leadership.")
                self.state = "follower"
                self.role = 'worker'
                self.current_master_address = best_node
                self.leader_address = best_node
                self.last_heartbeat_time = time.monotonic()
                
                # Create stubs for the new master
                self._create_master_stubs(best_node)
                return

        logging.info(f"[{self.address}] Becoming master for term {self.current_term}.")
        self.role = 'master'
        self.state = "leader"
        self.leader_address = self.address
        self.current_master_address = self.address

        # Add MasterService and WorkerService if not already added
        if not self._master_service_added:
            replication_pb2_grpc.MasterServiceServicer.__init__(self)
            replication_pb2_grpc.add_MasterServiceServicer_to_server(self, self._server)
            self._master_service_added = True
            logging.info(f"[{self.address}] MasterServiceServicer added to server.")

        if not self._worker_service_added:
            replication_pb2_grpc.WorkerServiceServicer.__init__(self)
            replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
            self._worker_service_added = True
            logging.info(f"[{self.address}] WorkerServiceServicer added to server.")

        # Initialize worker stubs
        self._worker_stubs = {}
        for node_addr in self.known_nodes:
            if node_addr != self.address:
                self._create_stubs_for_node(node_addr)

        # Start master routines
        if self._master_announcement_task is None or self._master_announcement_task.done():
            self._master_announcement_task = asyncio.create_task(self._master_election_announcement_routine())
            self._background_tasks.append(self._master_announcement_task)
        if self._other_nodes_health_check_task is None or self._other_nodes_health_check_task.done():
            self._other_nodes_health_check_task = asyncio.create_task(self._check_other_nodes_health())
            self._background_tasks.append(self._other_nodes_health_check_task)

    async def _send_request_vote(self, node_stub: replication_pb2_grpc.NodeServiceStub, request: replication_pb2.VoteRequest, node_address: str) -> replication_pb2.VoteResponse:
        """Sends a RequestVote RPC to a node."""
        try:
            response = await asyncio.wait_for(node_stub.RequestVote(request), timeout=5)
            logging.info(f"[{self.address}] Received VoteResponse from {node_address}: {response.vote_granted}")
            return response
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.warning(f"[{self.address}] VoteRequest to {node_address} failed: {e}")
            return None
        except Exception as e:
            logging.error(f"[{self.address}] Unexpected error sending VoteRequest to {node_address}: {e}", exc_info=True)
            return None

    async def _request_vote_from_node(self, node_stub: replication_pb2_grpc.NodeServiceStub, node_address: str) -> bool:
        """Sends a VoteRequest to a node."""
        try:
            response = await asyncio.wait_for(
                node_stub.RequestVote(
                    replication_pb2.VoteRequest(
                        candidate_id=self.id,
                        candidate_address=self.address,
                        term=self.current_term
                    ),
                    timeout=2
                ),
                timeout=2
            )
            return response.vote_granted
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.warning(f"[{self.address}] Vote request to {node_address} failed: {e}")
            return False
        except Exception as e:
            logging.error(f"[{self.address}] Unexpected error requesting vote from {node_address}: {e}", exc_info=True)
            return False

    async def _master_election_announcement_routine(self):
        """Periodically announces this node as the master."""

        while self.role == 'master':  # Keep announcing while master
            logging.info(f"[{self.address}] Announcing self as master (Term: {self.current_term}).")
            announcement = replication_pb2.MasterAnnouncement(
                master_address=self.address,
                node_id_of_master=self.id,
                term=self.current_term
            )

            announcement_tasks = []
            for node_addr in list(self._node_stubs):
                if node_addr == self.address:
                    continue
                try:
                    await self._send_master_announcement(node_addr, announcement)
                except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
                    logging.warning(f"[{self.address}] MasterAnnouncement to {node_addr} failed: {e}. Removing from cluster.")
                    # prune it
                    self._node_stubs.pop(node_addr, None)
                    self._worker_stubs.pop(node_addr, None)
                    self._channels.pop(node_addr, None)
                  

            if announcement_tasks:
                await asyncio.gather(*announcement_tasks, return_exceptions=True)

            await asyncio.sleep(5)  # Adjust interval as needed
        logging.info(f"[{self.address}] Master announcement routine stopped.")

    async def _send_master_announcement(self, node_address: str, announcement: replication_pb2.MasterAnnouncement):
        """Sends a MasterAnnouncement RPC to a node."""
        try:
            channel = self._get_or_create_channel(node_address)
            if channel:
                stub = replication_pb2_grpc.NodeServiceStub(channel)
                await asyncio.wait_for(stub.AnnounceMaster(announcement), timeout=5)
                logging.debug(f"[{self.address}] MasterAnnouncement sent successfully to {node_address}.")
            else:
                logging.warning(f"[{self.address}] Could not create channel to {node_address} for MasterAnnouncement.")
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.warning(f"[{self.address}] MasterAnnouncement to {node_address} failed: {e}")
        except Exception as e:
            logging.error(f"[{self.address}] Error sending MasterAnnouncement to {node_address}: {e}", exc_info=True)
            
    async def _start_election_with_delay(self):
        """Delays the election start by a randomized timeout."""
        if self._pre_election_delay_task and not self._pre_election_delay_task.done():
            self._pre_election_delay_task.cancel()  # Cancel any existing delay
        self.election_timeout = random.uniform(10, 15)
        logging.info(f"[{self.address}] Starting pre-election delay for {self.election_timeout:.2f} seconds.")
        self._pre_election_delay_task = asyncio.create_task(self._election_delay_coro())
        self._background_tasks.append(self._pre_election_delay_task)

    async def _election_delay_coro(self):
        """Handles election delay with deadlock prevention"""
        try:
            await asyncio.sleep(self.election_timeout)
            
            # Check if another node started election while we were waiting
            if self.state != "follower" or self.current_master_address:
                logging.info(f"[{self.address}] Aborting election - cluster state changed during delay")
                return
                
            # Force election resolution after too many attempts
            if hasattr(self, 'election_attempts') and self.election_attempts > 3:
                logging.warning(f"[{self.address}] Detected potential election deadlock after {self.election_attempts} attempts")
                
                # Force resolution based on node address - deterministic across cluster
                for node_addr in sorted(self.known_nodes + [self.address]):
                    if node_addr == self.address:
                        # We're the lowest node ID that's still alive - become leader
                        logging.info(f"[{self.address}] Forcing election resolution - becoming master by ID priority")
                        await self._become_master()
                        return
                    
                    # Check if this node with better ID is alive
                    if self._validate_stub(node_addr):
                        logging.info(f"[{self.address}] Detected alive node {node_addr} with better ID priority")
                        break
            
            # Normal election start
            await self.start_election()
        except asyncio.CancelledError:
            logging.info(f"[{self.address}] Pre-election delay cancelled.")


    async def retry_register_with_master(self, interval: float = 5.0):
        """Periodically attempts to register with master if not yet successful."""
        while self.role == 'worker':
            try:
                # Ensure the master stub exists or recreate it if needed
                if not self.master_stub:
                    self._create_master_stubs(self.current_master_address)
                       # Build and send the registration request to the master
                req = replication_pb2.RegisterWorkerRequest(worker_address=self.address)
                resp = await self.master_stub.RegisterWorker(req)
                if resp.success:
                    logging.info(f"[{self.address}] Successfully registered with master on retry: {resp.message}")
                    return  # Stop retrying after success
                else:
                    logging.info(f"[{self.address}] Retry registration response: {resp.message}")
            except Exception as e:
                logging.warning(f"[{self.address}] Retry register failed: {e}")
            await asyncio.sleep(interval)

    async def check_master_health(self):
        """Periodically checks the health of the master node."""
        while self.role == 'worker': 
            if not self.current_master_address:
                logging.info(f"[{self.address}] No master known, waiting briefly before checking again")
                await asyncio.sleep(2)
                continue
                
            try:
                # Get a NodeServiceStub for the master
                master_node_channel = self._get_or_create_channel(self.current_master_address)
                if not master_node_channel:
                    logging.warning(f"[{self.address}] Could not get channel to master at {self.current_master_address} for health check.")
                    self.current_master_address = None  # Clear the master address
                    await self._start_election_with_delay()  # Start election
                    continue  # Continue the loop instead of returning
                    
                master_node_stub = replication_pb2_grpc.NodeServiceStub(master_node_channel)

                response = await asyncio.wait_for(
                    master_node_stub.GetNodeStats(replication_pb2.NodeStatsRequest()),
                    timeout=5
                )
                
                # Update our knowledge of the network based on master's stats
                if response.known_nodes_count > len(self.known_nodes) + 1:  # +1 for self
                    logging.info(f"[{self.address}] Master knows more nodes than we do. Requesting update.")
                    await self._request_node_list_update(master_node_stub)
                    
                logging.debug(f"[{self.address}] Master at {self.current_master_address} is healthy.")
                self.last_heartbeat_time = time.monotonic()  # Update heartbeat
            except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
                logging.error(f"[{self.address}] Master unreachable: {e}")
                time_since_last_heartbeat = time.monotonic() - self.last_heartbeat_time
                
                if time_since_last_heartbeat > self.election_timeout:
                    # Remove failed master from all tracking
                    failed_master = self.current_master_address
                    if failed_master in self.known_nodes:
                        self.known_nodes.remove(failed_master)
                    if failed_master in self._node_stubs:
                        del self._node_stubs[failed_master]
                    if failed_master in self._channels:
                        await self._channels[failed_master].close()
                        del self._channels[failed_master]
                    
                    self.current_master_address = None
                    logging.info(f"[{self.address}] Init election after master {failed_master} removal")
                    await self._start_election_with_delay()
                    # break
            except Exception as e:
                logging.error(f"[{self.address}] Error checking master health: {e}", exc_info=True)
            await asyncio.sleep(2)  # Adjust as needed
        logging.info(f"[{self.address}] Master health check routine stopped.")

    async def _request_node_list_update(self, master_node_stub):
        """Request an updated node list from the master."""
        try:
            response = await master_node_stub.GetAllNodes(replication_pb2.GetAllNodesRequest())
            for node_info in response.nodes:
                node_addr = f"{node_info.address}:{node_info.port}"
                if node_addr != self.address and node_addr not in self.known_nodes:
                    logging.info(f"[{self.address}] Adding newly discovered node {node_addr} to known_nodes")
                    self.known_nodes.append(node_addr)
                    self._create_stubs_for_node(node_addr)
        except Exception as e:
            logging.error(f"[{self.address}] Failed to get node list from master: {e}")


async def serve(host: str, port: int, role: str, master_address: Optional[str], known_nodes: List[str]):
    """Starts the gRPC server and initializes the node."""
    node_instance = Node(host, port, role, master_address, known_nodes)
    await node_instance.start()
    return node_instance # Return the node instance for shutdown


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Distributed Video Encoding Node")
    parser.add_argument("--host", type=str, default="localhost", help="Host address to bind the server to")
    parser.add_argument("--port", type=int, required=True, help="Port to bind the server to")
    parser.add_argument("--role", type=str, choices=['master', 'worker'], required=True, help="Role of the node (master or worker)")
    parser.add_argument("--master", type=str, help="Address of the initial master node (host:port). Required for workers.")
    parser.add_argument("--nodes", type=str, nargs='*', default=[], help="List of known node addresses (host:port) in the network.")

    args = parser.parse_args()

    if args.role == 'worker' and not args.master:
        print("Error: --master is required for worker nodes.")
        sys.exit(1)

    node_address_arg = f"{args.host}:{args.port}"

    if args.role == 'worker' and args.master:
        if args.master not in args.nodes and args.master != node_address_arg:
            logging.info(f"[{node_address_arg}] Adding specified master {args.master} to list of connectable nodes for NodeServices.")
            args.nodes.append(args.master)
            args.nodes = list(set(args.nodes))

    node_instance = None # Initialize node_instance to None
    try:
        # Run the serve coroutine and get the node instance
        node_instance = asyncio.run(serve(args.host, args.port,
                                        args.role, args.master, args.nodes))
    except KeyboardInterrupt:
        print(f"\n[{args.host}:{args.port}] Node interrupted by user.")
    except Exception as e:
        logging.error(f"[{args.host}:{args.port}] Node execution failed: {type(e).__name__} - {e}", exc_info=True)
    finally:
        # Ensure graceful shutdown is attempted even if an exception occurred
        if node_instance:
             logging.info(f"[{args.host}:{args.port}] Attempting graceful shutdown.")
             # Need a new event loop to run the async stop method if the main loop is closed
             try:
                 loop = asyncio.get_running_loop()
             except RuntimeError:
                 # If no running loop, create a new one
                 loop = asyncio.new_event_loop()
                 asyncio.set_event_loop(loop)

             # Run the async stop method in the event loop
             loop.run_until_complete(node_instance.stop())
             # Close the new loop if we created it
             if loop != asyncio.get_event_loop_policy().get_event_loop(): # Check if it's not the default loop
                  loop.close()

        logging.info(f"[{args.host}:{args.port}] Node process finished.")