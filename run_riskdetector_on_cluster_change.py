import os
import subprocess
import sys
import time
import signal
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- Configuration ---
CSV_FILENAME = "cluster_summaries.csv"
TARGET_SCRIPT_FILENAME = "risk_detector.py"
LOG_FILENAME = "risk_detector.log"

# Resolve paths relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, CSV_FILENAME)
TARGET_SCRIPT_PATH = os.path.join(SCRIPT_DIR, TARGET_SCRIPT_FILENAME)
LOG_FILE_PATH = os.path.join(SCRIPT_DIR, LOG_FILENAME)
PID_FILE = os.path.join(SCRIPT_DIR, 'risk_detector.pid')

class ClusterCSVChangeHandler(FileSystemEventHandler):
    def __init__(self):
        self.last_line_count = self._current_line_count()
        self.running_process: subprocess.Popen | None = None
        self.cooldown_seconds = 5
        self.last_launch_time = 0
        self._cleanup_previous_process()

    def _terminate_pid(self, pid: int):
        try:
            os.kill(pid, signal.SIGTERM)
            start_time = time.time()
            while time.time() - start_time < 5:
                try:
                    os.kill(pid, 0)
                    time.sleep(0.5)
                except OSError:
                    break
            else:
                os.kill(pid, signal.SIGKILL)
                print(f"Watchdog: Force killed PID {pid} after timeout.")
            print(f"Watchdog: Terminated previous PID {pid}.")
        except ProcessLookupError:
            print(f"Watchdog: Previous PID {pid} not running.")
        except Exception as e:
            print(f"Watchdog: Error terminating PID {pid}: {e}")

    def _cleanup_previous_process(self):
        if os.path.exists(PID_FILE):
            try:
                with open(PID_FILE, 'r') as pf:
                    old_pid = int(pf.read().strip())
                self._terminate_pid(old_pid)
            except Exception as e:
                print(f"Watchdog: Failed to read PID file: {e}")
            finally:
                try:
                    os.remove(PID_FILE)
                except FileNotFoundError:
                    pass

    def _current_line_count(self) -> int:
        if not os.path.exists(CSV_FILE_PATH):
            return 0
        try:
            with open(CSV_FILE_PATH, "r", encoding="utf-8-sig") as f:
                return sum(1 for _ in f)
        except Exception:
            return 0

    def on_modified(self, event):
        if event.is_directory:
            return
        if os.path.abspath(str(event.src_path)) != CSV_FILE_PATH:
            return

        current_count = self._current_line_count()
        if current_count <= self.last_line_count:
            return

        self.last_line_count = current_count

        if self.running_process and self.running_process.poll() is None:
            print(f"Watchdog: Terminating previous '{TARGET_SCRIPT_FILENAME}' (PID: {self.running_process.pid})...")
            self._terminate_pid(self.running_process.pid)
            self.running_process = None
            try:
                os.remove(PID_FILE)
            except FileNotFoundError:
                pass

        current_time = time.time()
        if current_time - self.last_launch_time < self.cooldown_seconds:
            print("Watchdog: In cooldown. Skipping risk detector launch.")
            return

        self.last_launch_time = current_time
        if not os.path.exists(TARGET_SCRIPT_PATH):
            print(f"Watchdog: Target script '{TARGET_SCRIPT_PATH}' not found.")
            return

        try:
            with open(LOG_FILE_PATH, "a", encoding="utf-8") as logf:
                logf.write(f"\n--- Launching {TARGET_SCRIPT_FILENAME} at {time.asctime()} due to new cluster summary line ---\n")
                self.running_process = subprocess.Popen(
                    [sys.executable, TARGET_SCRIPT_PATH],
                    stdout=logf,
                    stderr=logf,
                    text=True,
                    cwd=SCRIPT_DIR
                )
                with open(PID_FILE, 'w') as pf:
                    pf.write(str(self.running_process.pid))
            print(f"Watchdog: Started '{TARGET_SCRIPT_FILENAME}' with PID {self.running_process.pid}.")
        except Exception as e:
            print(f"Watchdog: Failed to launch '{TARGET_SCRIPT_FILENAME}': {e}")

if __name__ == "__main__":
    if not os.path.exists(os.path.dirname(CSV_FILE_PATH)):
        print(f"Watchdog: Directory for CSV '{CSV_FILE_PATH}' does not exist. Exiting.")
        sys.exit(1)

    handler = ClusterCSVChangeHandler()
    observer = Observer()
    observer.schedule(handler, path=os.path.dirname(CSV_FILE_PATH), recursive=False)

    print(f"--- Cluster CSV Watchdog Initialised ---")
    print(f"Monitoring: {CSV_FILE_PATH}")
    print(f"Target script: {TARGET_SCRIPT_PATH}")
    print("Press Ctrl+C to stop.")

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nWatchdog: KeyboardInterrupt received. Stopping observer...")
    finally:
        observer.stop()
        observer.join()
        if handler.running_process and handler.running_process.poll() is None:
            print(f"Watchdog: Terminating running '{TARGET_SCRIPT_FILENAME}' (PID: {handler.running_process.pid})...")
            handler.running_process.terminate()
            try:
                handler.running_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                handler.running_process.kill()
        if os.path.exists(PID_FILE):
            try:
                os.remove(PID_FILE)
            except Exception:
                pass
        print("Watchdog: Exiting.")
