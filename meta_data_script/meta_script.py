import os
import datetime
from pymongo import MongoClient
from paramiko import SSHClient, AutoAddPolicy
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Connect to MongoDB
client = MongoClient(os.getenv("MONGO_URI"))
db = client["capstoneDB"]
raw_collection = db["CleanedData"]
uncleaned_collection = db["UncleanedData"]

# Set up SSH connection to the VM
ssh = SSHClient()
ssh.set_missing_host_key_policy(AutoAddPolicy())
ssh.connect(
    hostname=os.getenv("VM_IP"),
    username=os.getenv("VM_USERNAME"),
    password=os.getenv("VM_PASSWORD")
)

def parse_size(size_str):
    """Convert a human-readable size string to bytes."""
    if size_str[-1] in ['K', 'M', 'G', 'T']:
        unit = size_str[-1]
        size_value = float(size_str[:-1])
        if unit == 'K':
            return size_value * 1024
        elif unit == 'M':
            return size_value * (1024 ** 2)
        elif unit == 'G':
            return size_value * (1024 ** 3)
        elif unit == 'T':
            return size_value * (1024 ** 4)
    else:
        return int(size_str)  # No unit means it's already in bytes

def get_file_metadata(directory):
    """Retrieve file metadata from the directory on the VM."""
    stdin, stdout, stderr = ssh.exec_command(f"ls -lh {directory}")
    files = []
    for line in stdout:
        parts = line.split()
        if len(parts) > 8 and parts[-1].endswith(".csv"):
            size_in_bytes = parse_size(parts[4])
            files.append({
                "filename": parts[-1],
                "sizeInMB": round(size_in_bytes / (1024 * 1024), 2),
                "filepath": os.path.join(directory, parts[-1])
            })
    return files

def insert_metadata(files, collection):
    """Insert metadata into the specified MongoDB collection."""
    for file in files:
        if not collection.find_one({"filename": file["filename"]}):
            collection.insert_one({
                "filename": file["filename"],
                "filepath": file["filepath"],
                "sizeInMB": file["sizeInMB"],
                "uploadDate": datetime.datetime.now()
            })
            print(f"Inserted metadata for {file['filename']}")

# Scan and insert metadata for both raw and uncleaned files
raw_files = get_file_metadata(os.getenv("RAW_DIR"))
uncleaned_files = get_file_metadata(os.getenv("UNCLEANED_DIR"))

insert_metadata(raw_files, raw_collection)
insert_metadata(uncleaned_files, uncleaned_collection)

# Close SSH connection
ssh.close()