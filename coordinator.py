import argparse
import asyncio
import aiohttp
import json
from typing import List
from aiohttp import web


class Coordinator:
    """Manages workers and aggregates results"""

    def __init__(self, port: int):
        self.port = port
        self.workers = {}  # Dictionary to track workers
        self.results = []  # Store results from workers

    def start(self) -> None:
        """Start the coordinator server"""
        print(f"Starting coordinator on port {self.port}...")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._start_server())

    async def _start_server(self) -> None:
        """Start the aiohttp web server to handle requests from workers"""
        app = web.Application()
        app.router.add_post("/register", self.handle_worker_registration)
        app.router.add_post("/process_chunk", self.handle_process_chunk)
        app.router.add_post("/heartbeat", self.handle_worker_health_check)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", self.port)
        await site.start()

        print(f"Coordinator server running on http://localhost:{self.port}")
        while True:
            await asyncio.sleep(3600)  # Keep the server running

    async def handle_worker_registration(self, request: web.Request) -> web.Response:
        """Register a new worker"""
        data = await request.json()
        worker_id = data["worker_id"]
        worker_url = data["worker_url"]

        # Register the worker
        self.workers[worker_id] = {"worker_url": worker_url, "status": "active"}
        print(f"Worker {worker_id} registered.")
        return web.Response(text=f"Worker {worker_id} registered.")

    async def handle_worker_health_check(self, request: web.Request) -> web.Response:
        """Receive health check from workers"""
        data = await request.json()
        worker_id = data["worker_id"]

        if worker_id in self.workers:
            self.workers[worker_id]["status"] = "active"
            print(f"Health check passed for worker {worker_id}.")
        return web.Response(text=f"Health check received for worker {worker_id}.")

    async def handle_process_chunk(self, request: web.Request) -> web.Response:
        """Distribute chunk processing request to workers"""
        data = await request.json()
        filepath = data["filepath"]
        chunk_size = data["chunk_size"]

        # Split the file into chunks and send to workers
        file_size = os.path.getsize(filepath)
        chunks = self._split_file(filepath, file_size, chunk_size)

        results = []
        for i, chunk in enumerate(chunks):
            worker_id = list(self.workers.keys())[i % len(self.workers)]  # Round-robin assignment
            worker_url = self.workers[worker_id]["worker_url"]

            # Send the chunk to the worker
            chunk_data = {"filepath": filepath, "start": chunk[0], "size": chunk[1]}
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{worker_url}/process", json=chunk_data) as resp:
                    worker_result = await resp.json()
                    results.append(worker_result)

        # Aggregate the results from all workers
        aggregated_result = self.aggregate_results(results)
        return web.json_response(aggregated_result)

    def _split_file(self, filepath: str, file_size: int, chunk_size: int) -> List[tuple]:
        """Split the log file into chunks"""
        chunks = []
        start = 0
        while start < file_size:
            chunks.append((start, chunk_size))
            start += chunk_size
        return chunks

    def aggregate_results(self, results: List[dict]) -> dict:
        """Aggregate the results from workers"""
        aggregated = {
            "avg_response_time": 0,
            "requests_per_second": 0,
            "error_rate": 0.0,
            "malformed_lines": 0,
        }

        total_requests = 0
        total_time = 0
        total_malformed = 0
        total_chunks = len(results)

        for result in results:
            total_requests += result["requests_per_second"]
            total_time += result["avg_response_time"] * result["requests_per_second"]
            total_malformed += result["malformed_lines"]

        aggregated["requests_per_second"] = total_requests
        aggregated["avg_response_time"] = total_time / total_requests if total_requests > 0 else 0
        aggregated["malformed_lines"] = total_malformed
        return aggregated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    args = parser.parse_args()

    coordinator = Coordinator(port=args.port)
    coordinator.start()
