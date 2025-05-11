import unittest
import subprocess
import os
import time
import shutil

class TestMaster(unittest.TestCase):
    worker1_process = None
    worker2_process = None
    # Assuming test_master.py is in video_encoding_cmpe275-main/Master/
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # video_encoding_cmpe275-main
    worker_dir = os.path.join(base_dir, 'Worker')
    master_dir = os.path.join(base_dir, 'Master')
    sample_video_name = "file_example_MP4_1920_18MG.mp4"
    sample_video_path = os.path.join(master_dir, "sample_videos", sample_video_name)

    @classmethod
    def setUpClass(cls):
        print(f"Base directory: {cls.base_dir}")
        print(f"Worker directory: {cls.worker_dir}")
        print(f"Master directory: {cls.master_dir}")
        print(f"Sample video path: {cls.sample_video_path}")

        # Ensure worker shard directory is clean before test
        cls.worker_shards_dir = os.path.join(cls.worker_dir, "video_shards")
        if os.path.exists(cls.worker_shards_dir):
            print(f"Cleaning up existing shards directory: {cls.worker_shards_dir}")
            shutil.rmtree(cls.worker_shards_dir)
        os.makedirs(cls.worker_shards_dir, exist_ok=True)

        print(f"Starting worker 1 from {cls.worker_dir} on port 50061...")
        cls.worker1_process = subprocess.Popen(
            ['python3', 'worker.py', '--port', '50061'],
            cwd=cls.worker_dir,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        print(f"Starting worker 2 from {cls.worker_dir} on port 50062...")
        cls.worker2_process = subprocess.Popen(
            ['python3', 'worker.py', '--port', '50062'],
            cwd=cls.worker_dir,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        print("Waiting for workers to start (5 seconds)...")
        time.sleep(5)
        # Check if workers started
        if cls.worker1_process.poll() is not None:
            print("Worker 1 failed to start.")
            print("Worker 1 stdout:", cls.worker1_process.stdout.read().decode())
            print("Worker 1 stderr:", cls.worker1_process.stderr.read().decode())
        if cls.worker2_process.poll() is not None:
            print("Worker 2 failed to start.")
            print("Worker 2 stdout:", cls.worker2_process.stdout.read().decode())
            print("Worker 2 stderr:", cls.worker2_process.stderr.read().decode())


    @classmethod
    def tearDownClass(cls):
        print("Terminating worker processes...")
        if cls.worker1_process and cls.worker1_process.poll() is None:
            cls.worker1_process.terminate()
            cls.worker1_process.wait()
        if cls.worker2_process and cls.worker2_process.poll() is None:
            cls.worker2_process.terminate()
            cls.worker2_process.wait()
        print("Worker processes terminated.")

    def test_master_distributes_chunks(self):
        self.assertTrue(os.path.exists(self.sample_video_path), f"Sample video not found at {self.sample_video_path}")
        print(f"Running master.py from {self.master_dir} with video {self.sample_video_path}")
        
        master_command = [
            'python3',
            'master.py',
            '--video_path', self.sample_video_path,
            '--workers', 'localhost:50061,localhost:50062'
        ]
        print(f"Executing master command: {' '.join(master_command)}")

        master_process = subprocess.run(
            master_command,
            capture_output=True, text=True, cwd=self.master_dir
        )

        print("Master stdout:", master_process.stdout)
        print("Master stderr:", master_process.stderr)
        self.assertEqual(master_process.returncode, 0, f"Master script failed with error: {master_process.stderr}")
        self.assertIn("Distribution complete", master_process.stdout, "Master script output did not indicate completion.")

        self.assertTrue(os.path.isdir(self.worker_shards_dir), f"Shards directory {self.worker_shards_dir} not found.")
        
        found_shards = False
        if os.path.isdir(self.worker_shards_dir):
            for item in os.listdir(self.worker_shards_dir):
                if item.startswith(self.sample_video_name + "_chunk_"):
                    found_shards = True
                    break
        self.assertTrue(found_shards, f"No shard files found in {self.worker_shards_dir} for {self.sample_video_name}.")
        print(f"Shard files found in {self.worker_shards_dir}.")

if __name__ == '__main__':
    unittest.main()

