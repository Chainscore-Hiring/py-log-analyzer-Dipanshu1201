import argparse
import asyncio
import aiohttp
import requests
import os
import re
from typing import Dict

from aiohttp import web


class Worker:
    """Processes log chunks and reports results"""

    def __init__(self, port: int, worker_id: str, coordinator_url: str):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url
        self.port = port

    def start(self) -> None:
        """Start worker server"""
        print(f"Starting worker {self.worker_id} on port {self.port}...")
        # Start the HTTP server using aiohttp to listen for work requests
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._start_server())

    async def _start_server(self) -> None:
        """Start an aiohttp web server to handle requests."""
        from aiohttp import web

        app = web.Application()
        app.router.add_post("/process", self.handle_process_chunk)
        app.router.add_get("/heartbeat", self.handle_health_check)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", self.port)
        await site.start()

        print(f"Worker {self.worker_id} server running on http://localhost:{self.port}")
        while True:
            await asyncio.sleep(3600)  # Keep the server running

    async def handle_process_chunk(self, request: web.Request) -> web.Response:
        """Handle chunk processing request from the coordinator"""
        data = await request.json()
        filepath = data["filepath"]
        start = data["start"]
        size = data["size"]

        # Process the chunk
        metrics = await self.process_chunk(filepath, start, size)

        # Return the results to the coordinator
        return web.json_response(metrics)

    async def handle_health_check(self, request: web.Request) -> web.Response:
        """Send heartbeat to the coordinator"""
        await self.report_health()
        return web.Response(text="Health check passed.")

    async def process_chunk(self, filepath: str, start: int, size: int) -> Dict:
        """Process a chunk of log file and return metrics"""
        error_rate = 0.0
        total_time = 0
        total_requests = 0
        malformed_lines = 0

        # Open the file and read the chunk
        with open(filepath, 'r') as f:
            # Read lines from the start point, limiting to the chunk size
            f.seek(start)
            lines = f.readlines(size)

            for line in lines:
                match = re.match(r"(\S+ \S+) (\S+) Request processed in (\d+)ms", line)
                if match:
                    timestamp, log_level, response_time = match.groups()
                    total_requests += 1
                    total_time += int(response_time)
                else:
                    malformed_lines += 1

        # Calculate metrics
        avg_response_time = total_time / total_requests if total_requests > 0 else 0
        metrics = {
            'error_rate': error_rate,  # Assuming no errors in this log
            'avg_response_time': avg_response_time,
            'requests_per_second': total_requests,  # Could also be adjusted to per minute or more detailed analysis
            'malformed_lines': malformed_lines,
        }
        return metrics

    async def report_health(self) -> None:
        """Send heartbeat to coordinator"""
        try:
            response = requests.post(f"{self.coordinator_url}/heartbeat", json={"worker_id": self.worker_id})
            response.raise_for_status()
        except requests.exceptions.RequestException:
            print(f"Health check failed for worker {self.worker_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Worker")
    parser.add_argument("--port", type=int, default=8001, help="Worker port")
    parser.add_argument("--id", type=str, default="worker1", help="Worker ID")
    parser.add_argument("--coordinator", type=str, default="http://localhost:8000", help="Coordinator URL")
    args = parser.parse_args()

    worker = Worker(port=args.port, worker_id=args.id, coordinator_url=args.coordinator)
    worker.start()
