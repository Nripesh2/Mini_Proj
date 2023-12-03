import os
import boto3
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# AWS credentials and S3 bucket details
AWS_ACCESS_KEY = 'AKIAXQT3GSK6UP4YDZTD'
AWS_SECRET_KEY = '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'
AWS_REGION = 'ap-south-1'
S3_BUCKET_NAME = 'salescsvupload'
S3_BUCKET_PATH = 'data/'

# Function to upload a file to S3 bucket
def upload_to_s3(local_file_path, s3_file_name):
    # Initialize S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    # Uploading file to S3
    print(f"Uploading {local_file_path} to S3 bucket: {S3_BUCKET_NAME}/{S3_BUCKET_PATH}{s3_file_name}")
    s3.upload_file(local_file_path, S3_BUCKET_NAME, S3_BUCKET_PATH + s3_file_name)
    print(f"Upload complete for {local_file_path}")

# Watcher class to monitor file system events
class Watcher:
    def __init__(self, directory_to_watch):
        # Initialize Observer object
        self.observer = Observer()
        self.directory_to_watch = directory_to_watch
        self.keep_running = True  # Flag to control script execution
        self.last_activity_time = time.time()  # Initialize the last activity time

    def run(self):
        event_handler = Handler(self)  # Pass the Watcher instance to the Handler
        # Schedule the handler to watch the given directory for file system events
        self.observer.schedule(event_handler, self.directory_to_watch, recursive=True)
        self.observer.start()
        print(f"Watching directory: {self.directory_to_watch}")
        try:
            while self.keep_running:  # Check the flag to keep running
                if time.time() - self.last_activity_time > 1:  # Change 1 to desired timeout in seconds
                    # If no activity detected for a while, stop monitoring and uploading
                    print("\nNo activity detected for a while. Stopping the file monitoring and upload process...")
                    self.observer.stop()
                    self.keep_running = False  # Set flag to stop the loop
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping the file monitoring and upload process...")
            self.observer.stop()
            self.keep_running = False  # Set flag to stop the loop
        self.observer.join()

        # Final output after the loop ends
        if not self.keep_running:
            print("File monitoring stopped.")

    def update_activity_time(self):
        # Update the last activity time
        self.last_activity_time = time.time()

# Event handler for file system events
class Handler(FileSystemEventHandler):
    def __init__(self, watcher):
        self.watcher = watcher

    def on_created(self, event):
        if not event.is_directory:
            print(f'New file created: {event.src_path}')
            if 'Data' in event.src_path and event.src_path.lower().endswith('.csv'):
                # If a new CSV file is detected, upload it to S3
                file_name = os.path.basename(event.src_path)
                print(f"Detected a new CSV file: {file_name}")
                upload_to_s3(event.src_path, file_name)
                self.watcher.update_activity_time()  # Update activity time on file creation

    def on_modified(self, event):
        if not event.is_directory:
            print(f'File modified: {event.src_path}')
            if 'Data' in event.src_path and event.src_path.lower().endswith('.csv'):
                # If a CSV file is modified, upload the modified version to S3
                file_name = os.path.basename(event.src_path)
                print(f"CSV file modified: {file_name}")
                upload_to_s3(event.src_path, file_name)
                self.watcher.update_activity_time()  # Update activity time on file modification

if __name__ == "__main__":
    path_to_monitor = r'C:\Users\Rakshita\Downloads\Mini Project-Atgeir Solutions\Data'

    w = Watcher(path_to_monitor)
    print("Starting file monitoring and upload process...")

    # Start the monitoring process in a separate thread
    monitor_thread = threading.Thread(target=w.run)
    monitor_thread.start()

    try:
        while True:
            # Update the activity time whenever there's an iteration in the main thread
            w.update_activity_time()
            time.sleep(1)

            # Check if there's been no activity for a specific duration (e.g., 1 second)
            if time.time() - w.last_activity_time > 1:
                w.keep_running = False  # Stop the monitoring process
                break  # Exit the loop
    except KeyboardInterrupt:
        w.keep_running = False  # Stop the monitoring process
        monitor_thread.join()
        print("File monitoring stopped due to keyboard interruption.")

    # Wait for the monitoring thread to finish
    monitor_thread.join()
    print("File monitoring stopped.")
