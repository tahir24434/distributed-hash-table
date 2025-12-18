'''
In a distributed hash table, if you use hash(key) % num_nodes, adding/removing a node remaps almost ALL keys. 
Consistent hashing solution: Only keys between the removed node and its predecessor need to move, minimizing disruption.

The ConsistentHashing class acts as a smart load balancer that determines which cluster(group of replicas) should handle each key in your 
distributed hash table system.

Virtual Ring: Imagine a circular number line from 0 to ~4 billion (32-bit hash space)
Multiple Virtual Nodes: Each physical node gets placed at multiple positions on the ring (controlled by multiplier=10)
Key Assignment: To find which node owns a key, a/ hash the key to find its positoin on the ring. 
                b/ Binary search (bisect_left) to find the next node clockwise c/ Uses modulo to wrap around the ring.
'''

from sortedcontainers import SortedList
import mmh3, time
from threading import Lock

class ConsistentHashing:
    def __init__(self, multiplier=10):
        # The Ring (node_hashes):
        # A sorted list of (hash_value, node_id) tuples.
        # Sorted by hash value to enable efficient binary search
        self.node_hashes = SortedList(key=lambda x: x[0])

        # Number of virtual nodes per physical node
        # Each physical node creates multiplier virtual nodes (default 10)
        # node_id + "0", node_id + "1", ... node_id + "9" are hashed
        self.node_multiplier = multiplier
        self.lock = Lock()
    
    def add_node_hash(self, node_id):
        """
        Adds a new cluster/partition/shard (group of leader/replica nodes) on the consistent hash ring 
        by creating multiple virtual nodes. Cluster is just a logical group responsible for a subset of keys.

        Creates 'multiplier' virtual positions for the cluster to ensure even load distribution.
        Each virtual node is a different position on the ring (0 to 2^32-1), preventing clustering
        and ensuring keys are distributed uniformly across physical nodes.

        Calls at initialization of hashtable_service and when scaling out (adding more clusters/partitions/shards).
        """
        existing = self.node_exists(node_id)

        with self.lock:
            if existing is False:
                for i in range(self.node_multiplier):
                    h = mmh3.hash(node_id + str(i), signed=False)
                    self.node_hashes.add((h, node_id))
                return 1
            return -1
        
    def get_next_node(self, key):
        with self.lock:
            h = mmh3.hash(key, signed=False)
            if len(self.node_hashes) > 0:
                index = self.node_hashes.bisect_left((h, ''))
                return self.node_hashes[index % len(self.node_hashes)][1]
            return None
    
    def get_next_nodes_from_node(self, node_id):
        nodes = set()
        
        with self.lock:
            for i in range(self.node_multiplier):
                h = mmh3.hash(node_id + str(i), signed=False)
                index = self.node_hashes.bisect_left((h, node_id))
                next_node = self.node_hashes[(index+1) % len(self.node_hashes)][1]
                
                if next_node != node_id:
                    nodes.add(next_node)
                    
        return nodes
    
    def node_exists(self, node_id):
        with self.lock:
            for i in range(self.node_multiplier):
                h = mmh3.hash(node_id + str(i), signed=False)
                try:
                    j = self.node_hashes.index((h, node_id))
                except:
                    return False
        
            return True
        