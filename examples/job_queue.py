"""
AhanaFlow Job Queue Example

Background task processing with ENQUEUE/DEQUEUE.
Perfect for async workflows, email sending, image processing, etc.
"""

import time
import socket
import json
import random
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class Job:
    """Represents a background job."""
    job_id: str
    job_type: str
    payload: dict
    created_at: str


class JobQueue:
    """Job queue using AhanaFlow's FIFO queue commands."""
    
    def __init__(self, host: str = "localhost", port: int = 9633):
        self.host = host
        self.port = port
    
    def send_command(self, cmd: dict) -> dict:
        """Send command to AhanaFlow server."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.host, self.port))
            sock.sendall((json.dumps(cmd) + "\n").encode())
            response = sock.recv(16384).decode().strip()
            return json.loads(response)
        finally:
            sock.close()
    
    def enqueue(self, queue_name: str, job: Job) -> bool:
        """Add job to queue."""
        result = self.send_command({
            "cmd": "ENQUEUE",
            "queue": queue_name,
            "item": {
                "job_id": job.job_id,
                "job_type": job.job_type,
                "payload": job.payload,
                "created_at": job.created_at
            }
        })
        return result.get("status") == "ok"
    
    def dequeue(self, queue_name: str) -> Optional[Job]:
        """Remove job from queue."""
        result = self.send_command({
            "cmd": "DEQUEUE",
            "queue": queue_name
        })
        
        if result.get("status") == "ok" and result.get("result"):
            item = result["result"]
            return Job(
                job_id=item["job_id"],
                job_type=item["job_type"],
                payload=item["payload"],
                created_at=item["created_at"]
            )
        return None
    
    def queue_length(self, queue_name: str) -> int:
        """Get queue length."""
        result = self.send_command({
            "cmd": "QLEN",
            "queue": queue_name
        })
        return result.get("result", 0)


def process_email_job(job: Job):
    """Simulate email sending."""
    print(f"  📧 Sending email to {job.payload['to']}")
    time.sleep(random.uniform(0.5, 2.0))  # Simulate API call
    print(f"  ✓ Email sent to {job.payload['to']}")


def process_resize_job(job: Job):
    """Simulate image resizing."""
    print(f"  🖼️  Resizing image {job.payload['image_path']}")
    time.sleep(random.uniform(1.0, 3.0))  # Simulate image processing
    print(f"  ✓ Image resized: {job.payload['image_path']}")


def process_webhook_job(job: Job):
    """Simulate webhook delivery."""
    print(f"  🔔 Sending webhook to {job.payload['url']}")
    time.sleep(random.uniform(0.3, 1.5))  # Simulate HTTP request
    print(f"  ✓ Webhook delivered: {job.payload['url']}")


# Job processors
JOB_PROCESSORS = {
    "email": process_email_job,
    "resize_image": process_resize_job,
    "webhook": process_webhook_job,
}


def worker(queue: JobQueue, queue_name: str):
    """Worker process that consumes jobs from queue."""
    print(f"\n🔄 Worker started, listening to '{queue_name}' queue...")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            job = queue.dequeue(queue_name)
            
            if job:
                print(f"\n▶ Processing job {job.job_id} ({job.job_type})")
                
                processor = JOB_PROCESSORS.get(job.job_type)
                if processor:
                    try:
                        processor(job)
                    except Exception as e:
                        print(f"  ✗ Job failed: {e}")
                else:
                    print(f"  ⚠ Unknown job type: {job.job_type}")
            else:
                # No jobs available, wait before polling again
                time.sleep(0.5)
    
    except KeyboardInterrupt:
        print("\n\n🛑 Worker stopped")


# Example usage
if __name__ == "__main__":
    import sys
    
    queue = JobQueue()
    queue_name = "background_jobs"
    
    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        # Run as worker
        worker(queue, queue_name)
    else:
        # Enqueue sample jobs (producer)
        print("="*60)
        print("AhanaFlow Job Queue Demo")
        print("="*60 + "\n")
        
        # Sample jobs
        jobs = [
            Job(
                job_id="job_001",
                job_type="email",
                payload={"to": "alice@example.com", "subject": "Welcome!"},
                created_at=datetime.now().isoformat()
            ),
            Job(
                job_id="job_002",
                job_type="resize_image",
                payload={"image_path": "/uploads/photo.jpg", "size": "800x600"},
                created_at=datetime.now().isoformat()
            ),
            Job(
                job_id="job_003",
                job_type="webhook",
                payload={"url": "https://example.com/webhook", "event": "user.created"},
                created_at=datetime.now().isoformat()
            ),
            Job(
                job_id="job_004",
                job_type="email",
                payload={"to": "bob@example.com", "subject": "Password reset"},
                created_at=datetime.now().isoformat()
            ),
        ]
        
        # Enqueue jobs
        print("📥 Enqueuing jobs...\n")
        for job in jobs:
            success = queue.enqueue(queue_name, job)
            status = "✓" if success else "✗"
            print(f"  {status} {job.job_id}: {job.job_type}")
        
        # Check queue length
        queue_len = queue.queue_length(queue_name)
        print(f"\n✓ {queue_len} jobs in queue\n")
        
        print("="*60)
        print(f"Run worker: python {sys.argv[0]} worker")
        print("="*60)
