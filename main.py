import logging
import os
import threading
import time
from typing import Optional

from dotenv import load_dotenv

from datastructures.niko import NikoDataConverter
from modules.niko_home_control import NikoHomeControlAPI

load_dotenv()


class NikoService:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379,
                 niko_controller: Optional['NikoHomeControlAPI'] = None):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.niko_controller = niko_controller
        self.logger = logging.getLogger(__name__)
        self.sensors = {}
        self.running = False
        self.thread = None

        # Initialize Redis publisher
        # self.redis_publisher = RedisPublisher(redis_host, redis_port)

        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def start(self):
        """Start the background monitoring and control thread."""
        if self.running:
            return

        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.info("Controller gestart")

    def _run_loop(self):
        """Main loop to regularly update each screen."""
        while self.running:
            try:
                self.devices = self.niko_controller.list_devices()
                self.logger.info("Found %d devices", len(self.devices))
                print(f"{self.devices=}")

                if self.devices:
                    return [NikoDataConverter.create_device(d) for d in self.devices ]

            except Exception as e:
                self.logger.error(f"Error in run loop: {str(e)}")
                # Wait longer on error to avoid spamming
                time.sleep(10)
            else:
                time.sleep(5)  # Normal wait time
        return None

    def stop(self):
        """Stop the background thread gracefully."""
        self.running = False
        if self.thread:
            self.thread.join()
        self.logger.info("Controller gestopt")

if __name__ == "__main__":
    niko = NikoHomeControlAPI(
        host=os.getenv("HOSTNAME"),
        username="hobby",
        jwt_token=os.getenv("JWT_TOKEN"),
        # ca_cert_path=str(Path(__file__).parent / 'modules' / "ca-chain.cert.pem")
        ca_cert_path='/ca-chain.cert.pem'
    )

    niko_service = NikoService(niko_controller = niko)
    niko_service.start()
    print("Niko controller started. Press Ctrl+C to stop.")

    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        niko_service.stop()
        niko.close()
        print("Application stopped.")