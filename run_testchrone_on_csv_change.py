import time
import os
import subprocess
import sys
import signal
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- Configuration ---
# Name of the CSV file to monitor
CSV_FILENAME = "sniperx_results_1m.csv"
# Name of the Python script to launch (assumed to be in the same directory as this watchdog script)
TARGET_SCRIPT_FILENAME = "test_chrome.py"
# Name of the log file for the target script's output
TARGET_SCRIPT_LOG_FILENAME = "test_chrome_output.log" # Log for the Bubblemaps script
# Cooldown period in seconds between launches - This watchdog doesn't use a cooldown itself,
# but prevents re-launch if target is running. Target script handles its own processing rate.

# --- Resolve full paths ---
# Directory where this watchdog script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Absolute path to the CSV file
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, CSV_FILENAME)

# Absolute path to the target Python script
TARGET_SCRIPT_PATH = os.path.join(SCRIPT_DIR, TARGET_SCRIPT_FILENAME)
# Absolute path to the log file for the target script
TARGET_SCRIPT_LOG_PATH = os.path.join(SCRIPT_DIR, TARGET_SCRIPT_LOG_FILENAME)
PID_FILE = os.path.join(SCRIPT_DIR, 'test_chrome.pid')


class CSVChangeHandler(FileSystemEventHandler):
    def __init__(self):
        self.last_mtime = 0
        self.running_process = None  # Stores the subprocess.Popen object
        self.last_launch_time = 0 # To implement a simple cooldown for watchdog itself if needed
        self.cooldown_seconds = 5 # Cooldown for watchdog reacting to multiple quick changes
        self._cleanup_previous_process()

    def _terminate_pid(self, pid: int):
        try:
            # First try a gentle termination
            try:
                os.kill(pid, signal.CTRL_BREAK_EVENT if os.name == 'nt' else signal.SIGTERM)
                print(f"Watchdog: Sent termination signal to PID {pid}...")
                
                # Wait for process to terminate
                start_time = time.time()
                while time.time() - start_time < 5:  # 5 second timeout
                    try:
                        os.kill(pid, 0)  # Check if process exists
                        time.sleep(0.2)
                    except (ProcessLookupError, OSError):
                        print(f"Watchdog: PID {pid} terminated gracefully.")
                        return
                
                # If we get here, process didn't terminate, force kill it
                print(f"Watchdog: Process {pid} did not terminate gracefully, forcing...")
                try:
                    import psutil
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    for child in children:
                        try:
                            child.terminate()
                        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                            pass
                    
                    # Give children a moment to terminate
                    gone, still_alive = psutil.wait_procs(children, timeout=3)
                    for p in still_alive:
                        try:
                            p.kill()
                        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                            pass
                    
                    # Now terminate the parent
                    if os.name == 'nt':
                        # On Windows, use taskkill to terminate the process tree
                        try:
                            subprocess.run(['taskkill', '/F', '/T', '/PID', str(pid)], 
                                        timeout=3, capture_output=True, text=True)
                            print(f"Watchdog: Terminated process tree for PID {pid} using taskkill")
                        except (subprocess.SubprocessError, FileNotFoundError) as e:
                            print(f"Watchdog: taskkill failed for PID {pid}, falling back to terminate(): {e}")
                            parent.terminate()
                    else:
                        # On Unix-like systems
                        parent.terminate()
                    
                    try:
                        parent.wait(3)
                        print(f"Watchdog: Process group for PID {pid} terminated")
                    except (subprocess.TimeoutExpired, psutil.TimeoutExpired):
                        print(f"Watchdog: Process group for PID {pid} did not terminate in time")
                
                except Exception as e:
                    print(f"Watchdog: Error in process tree termination for {pid}: {e}")
                    # Fall back to simple kill if psutil fails
                    try:
                        if os.name == 'nt':
                            subprocess.run(['taskkill', '/F', '/PID', str(pid)], 
                                        timeout=3, capture_output=True, text=True)
                        else:
                            os.kill(pid, signal.SIGKILL)
                    except Exception as e2:
                        print(f"Watchdog: Error force killing PID {pid}: {e2}")
                
            except ProcessLookupError:
                print(f"Watchdog: Process {pid} not found, already terminated.")
            except Exception as e:
                print(f"Watchdog: Error during initial termination of {pid}: {e}")
                
        except Exception as e:
            print(f"Watchdog: Unexpected error in _terminate_pid for {pid}: {e}")

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

    def on_modified(self, event):
        if event.is_directory:
            return

        try:
            event_abs_path = os.path.abspath(str(event.src_path))
        except Exception:
            return

        if event_abs_path == CSV_FILE_PATH:
            try:
                current_mtime = os.path.getmtime(CSV_FILE_PATH)
            except FileNotFoundError:
                print(f"Watchdog: Monitored CSV file '{CSV_FILE_PATH}' seems to have been deleted.")
                self.last_mtime = 0 # Reset mtime
                return

            if current_mtime == self.last_mtime:
                # print(f"Watchdog: '{CSV_FILENAME}' modified event, but mtime is unchanged ({current_mtime}). Skipping.")
                return # No actual content change likely
            
            self.last_mtime = current_mtime

            # Simple cooldown for watchdog itself to avoid rapid-fire launches from editor saves etc.
            current_time = time.time()
            if current_time - self.last_launch_time < self.cooldown_seconds:
                print(f"Watchdog: '{CSV_FILENAME}' changed, but still in cooldown. Skipping launch.")
                return

            if not os.path.exists(TARGET_SCRIPT_PATH):
                print(f"Watchdog: ERROR - Target script '{TARGET_SCRIPT_PATH}' not found. Cannot launch.")
                return

            if self.running_process and self.running_process.poll() is None:
                print(f"Watchdog: Terminating previous '{TARGET_SCRIPT_FILENAME}' (PID: {self.running_process.pid})...")
                try:
                    # Store the PID before cleanup
                    old_pid = self.running_process.pid
                    # Clear the reference first to avoid race conditions
                    proc = self.running_process
                    self.running_process = None
                    # Now terminate the process
                    self._terminate_pid(old_pid)
                    # Ensure the process object is cleaned up
                    try:
                        proc.wait(timeout=1)
                    except (subprocess.TimeoutExpired, AttributeError):
                        pass
                except Exception as e:
                    print(f"Watchdog: Error during process cleanup: {e}")
                finally:
                    try:
                        if os.path.exists(PID_FILE):
                            os.remove(PID_FILE)
                    except Exception as e:
                        print(f"Watchdog: Error removing PID file: {e}")

            print(f"Watchdog: '{CSV_FILENAME}' content changed (mtime: {current_mtime}). Launching '{TARGET_SCRIPT_FILENAME}'...")
            try:
                with open(TARGET_SCRIPT_LOG_PATH, "a", encoding="utf-8") as logf:
                    logf.write(f"\n--- Watchdog launching {TARGET_SCRIPT_FILENAME} at {time.asctime()} due to {CSV_FILENAME} change ---\n")
                    
                    # Create a new process group for the subprocess to isolate signals
                    creation_flags = 0
                    if os.name == 'nt':
                        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP
                    else:
                        # On Unix-like systems, we'll use preexec_fn to create a new process group
                        def preexec():
                            import os
                            os.setpgrp()
                    
                    self.running_process = subprocess.Popen(
                        [sys.executable, TARGET_SCRIPT_PATH],
                        stdout=logf,
                        stderr=logf,
                        text=True,
                        cwd=SCRIPT_DIR,
                        creationflags=creation_flags if os.name == 'nt' else 0,
                        preexec_fn=preexec if os.name != 'nt' else None
                    )
                    with open(PID_FILE, 'w') as pf:
                        pf.write(str(self.running_process.pid))
                    self.last_launch_time = current_time # Update last launch time
                    print(f"Watchdog: Successfully launched '{TARGET_SCRIPT_FILENAME}' with PID {self.running_process.pid}. "
                          f"Output logged to '{TARGET_SCRIPT_LOG_PATH}'.")
            except Exception as e:
                print(f"Watchdog: ERROR - Failed to launch '{TARGET_SCRIPT_FILENAME}': {e}")


if __name__ == "__main__":
    # Configure signal handlers early to prevent KeyboardInterrupt during initialization
    import signal
    
    def signal_handler(sig, frame):
        print("\nWatchdog: Received shutdown signal. Cleaning up...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Watchdog: WARNING - Monitored CSV file '{CSV_FILE_PATH}' does not exist. "
              "Waiting for it to be created by SniperX V2.")

    watch_directory = os.path.dirname(CSV_FILE_PATH)
    if not os.path.isdir(watch_directory):
        print(f"Watchdog: ERROR - Cannot watch directory '{watch_directory}' as it does not exist. Exiting.")
        sys.exit(1)

    print(f"--- Watchdog Initializing ---")
    print(f"Monitoring CSV file: '{CSV_FILE_PATH}'")
    print(f"In directory       : '{watch_directory}'")
    print(f"Target script      : '{TARGET_SCRIPT_PATH}'")
    print(f"Target script log  : '{TARGET_SCRIPT_LOG_PATH}'")
    print(f"---------------------------")

    event_handler = CSVChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path=watch_directory, recursive=False)
    
    print("Watchdog: Observer starting. Press Ctrl+C to stop.")
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nWatchdog: KeyboardInterrupt received. Stopping observer...")
    except Exception as e:
        print(f"Watchdog: An unexpected error occurred in the main loop: {e}")
    finally:
        observer.stop()
        print("Watchdog: Observer stopped.")
        observer.join()
        print("Watchdog: Observer joined.")

        if event_handler.running_process and event_handler.running_process.poll() is None:
            print(f"Watchdog: Terminating running target script '{TARGET_SCRIPT_FILENAME}' (PID: {event_handler.running_process.pid})...")
            event_handler.running_process.terminate()
            try:
                event_handler.running_process.wait(timeout=5)
                print(f"Watchdog: Target script PID {event_handler.running_process.pid} terminated gracefully.")
            except subprocess.TimeoutExpired:
                print(f"Watchdog: Target script PID {event_handler.running_process.pid} did not terminate gracefully. Sending SIGKILL...")
                event_handler.running_process.kill()
                try:
                    event_handler.running_process.wait(timeout=2)
                    print(f"Watchdog: Target script PID {event_handler.running_process.pid} killed.")
                except Exception as e_kill:
                    print(f"Watchdog: Error during SIGKILL for PID {event_handler.running_process.pid}: {e_kill}")
            except Exception as e_term:
                 print(f"Watchdog: Error during termination for PID {event_handler.running_process.pid}: {e_term}")
        if os.path.exists(PID_FILE):
            try:
                os.remove(PID_FILE)
            except Exception:
                pass
        print("Watchdog: Exiting.")