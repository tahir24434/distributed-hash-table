'''
The commit log provides local persistence (log, read_log, truncate) and 
network transfer (write_log_from_sock, send_log_to_sock) capabilities, 
enabling both durability and state replication in the distributed system.

commit_log a/ appends a timestamped commands(records) to the log file 
b/ Reads all commands from the log file c/ Recives log over network (write_log_from_sock) 
d/ Sends log over network (send_log_to_sock)

Durability: It persists all operations (commands) to a commit log file with timestamps on disk.
Replica Synchronization: Bootstrap new replicas with existing commit log data over a socket connection.
'''

from datetime import datetime
from threading import Lock
import os, tqdm

class CommitLog:
    def __init__(self, file='commit-log.txt'):
        self.file = file
        self.lock = Lock()
        
    def truncate(self):
        with self.lock:
            with open(self.file, 'w') as f:
                f.truncate()

    # Leader/Replicas uses it to persists.
    # self.ht.set(key=key, value=value, req_id=req_id)
    # self.commit_log.log(msg)  # Logs: "set key value req_id"    
    def log(self, command, sep=" "):
        with self.lock:
            with open(self.file, 'a') as f:
                now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                message = now + "," + command
                f.write(f"{message}\n")
    
    # Read records from file.
    # Replcas uses it to bootstrap from cloned commit log of leader.
    # write_log_from_sock(sock) is used to receive the log from the leader.
    #
    # commands = self.commit_log.read_log()
    # for cmd in commands: # Extract operation and replay on local hashtable. 
    #   self.ht.set(...) or self.ht.delete(...)
    #.  self.commit_log.log(...)  # Re-log the command locally.
    def read_log(self):
        with self.lock:
            output = []
            with open(self.file, 'r') as f:
                for line in f:
                    _, command = line.strip().split(",")
                    output += [command]
            
            return output
    
    '''
    Replicas uses it to download commit log from leader when joining.
    def join_replica(self):
        if self.is_leader is False:  # ← Only replicas
            self.commit_log.write_log_from_sock(sock)
    '''
    def write_log_from_sock(self, sock):
        with self.lock:
            file_name = self.file
            file_size = os.path.getsize(file_name)
            BUFFER_SIZE = 4096
            
            sock.send("commitlog".encode())
            
            progress = tqdm.tqdm(range(file_size), f"Receiving {file_name}", unit="B", unit_scale=True, unit_divisor=1024)
            
            with open(self.file, 'ab') as f:
                while True:
                    bytes_read = sock.recv(BUFFER_SIZE)
                    if not bytes_read:
                        break
                    f.write(bytes_read)
                    progress.update(len(bytes_read))

    '''
    Send commit log file.
    Leader uses it to upload commit log to replicas. 
    '''                
    def send_log_to_sock(self, sock):
        with self.lock:
            file_name = self.file
            file_size = os.path.getsize(file_name)
            BUFFER_SIZE = 4096
            
            progress = tqdm.tqdm(range(file_size), f"Sending {file_name}", unit="B", unit_scale=True, unit_divisor=1024)
            with open(file_name, "rb") as f:
                while True:
                    bytes_read = f.read(BUFFER_SIZE)
                    if not bytes_read:
                        break
                    sock.sendall(bytes_read)
                    progress.update(len(bytes_read))