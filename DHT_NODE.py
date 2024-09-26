""" Chord DHT node implementation. """
import socket
import threading
import logging
import pickle
from utils import dht_hash, contains
from sudoku import Sudoku
import time

HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 10

class FingerTable:
    """Finger Table."""

    def __init__(self, node_id, node_addr, m_bits=10):
        """ Initialize Finger Table."""
        
        self.node_id = node_id
        self.node_addr = node_addr
        self.m_bits = m_bits
        self.finger_table = [(node_id, node_addr) for _ in range(m_bits)]


    def fill(self, node_id, node_addr):
        """ Fill all entries of finger_table with node_id, node_addr."""
        self.finger_table = [(node_id, node_addr) for _ in range(self.m_bits)]

    def update(self, index, node_id, node_addr):
        """Update index of table with node_id and node_addr."""
        self.finger_table[index-1] = (node_id, node_addr)

    def find(self, identification):
        """ Get node address of closest preceding node (in finger table) of identification. """
        for i in range(-1,-1-self.m_bits,-1):
            if contains(self.finger_table[i][0], self.node_id, identification):
                return self.finger_table[i][1]
        return self.finger_table[0][1]

    def refresh(self):
        """ Retrieve finger table entries requiring refresh."""
        fingers = []
        for i in range(self.m_bits):
            id = (self.node_id + 2**i) % (2**self.m_bits)
            fingers.append((i+1, id, self.finger_table[i][1]))
        return fingers

    def getIdxFromId(self, id):
        """ Get index of finger table entry corresponding to id."""
        for i in range(self.m_bits):
            if contains(self.node_id, (self.node_id + 2**i) % (2**self.m_bits), id):
                return i+1

    def __repr__(self):
        return f"FingerTable: {self.finger_table}"

    @property
    def as_list(self):
        """return the finger table as a list of tuples: (identifier, (host, port)).
        NOTE: list index 0 corresponds to finger_table index 1
        """
        return [entry for entry in self.finger_table]
            

class DHTNode(threading.Thread):
    """ DHT Node Agent. """

    def __init__(self, address, dht_address=None, timeout=3, handicap=0):
        self.handicap = handicap
        self.done = False
        self.identification = dht_hash(address.__str__())
        self.addr = address  # My address
        self.dht_address = dht_address  # Address of the Node passed through the command line 
        # sudokus
        self.sudokuIds = []
        self.mySudokus = {}

        # stats
        self.stats = {}
        self.validations = 0
        self.solved = 0
        self.network = {}
        self.allNetwork = {}

        # nodes verifications
        self.last_heartbeat = {}
        self.time_verify = {} 

        # mutexs
        self.lock = 0
        self.blocker = threading.Event()

        if dht_address is None:
            self.inside_dht = True
            # I'm my own successor
            self.successor_id = self.identification
            self.successor_addr = address
            self.predecessor_id = None
            self.predecessor_addr = None
        else:
            self.inside_dht = False
            self.successor_id = None
            self.successor_addr = None
            self.predecessor_id = None
            self.predecessor_addr = None

        self.finger_table = FingerTable(self.identification, self.addr)   
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(timeout)
        self.socket.bind(self.addr)


    def send(self, address, msg):
        """ Send msg to address. """
        if address is None:
            return
        payload = pickle.dumps(msg)
        self.socket.sendto(payload, address)


    def recv(self):
        """ Retrieve msg payload and from address."""
        try:
            payload, addr = self.socket.recvfrom(1024)
        except socket.timeout:
            return None, None

        if len(payload) == 0:
            return None, addr
        return payload, addr


    def node_join(self, args):
        """Process JOIN_REQ message.

        Parameters:
            args (dict): addr and id of the node trying to join
        """
        addr = args["addr"]
        identification = args["id"]
        if self.identification == self.successor_id:  # I'm the only node in the DHT
            self.successor_id = identification
            self.successor_addr = addr
            self.finger_table.update(1, self.successor_id, self.successor_addr)

            args = {"successor_id": self.identification, "successor_addr": self.addr}
            self.send(addr, {"method": "JOIN_REP", "args": args, "sudokuIds": self.sudokuIds})

        elif contains(self.identification, self.successor_id, identification):
            args = {
                "successor_id": self.successor_id,
                "successor_addr": self.successor_addr,
            }
            self.successor_id = identification
            self.successor_addr = addr
            self.finger_table.update(1, self.successor_id, self.successor_addr)

            self.send(addr, {"method": "JOIN_REP", "args": args, "sudokuIds": self.sudokuIds})

        else:
            self.send(self.successor_addr, {"method": "JOIN_REQ", "args": args})


    def get_successor(self, args):
        """Process SUCCESSOR message.

        Parameters:
            args (dict): addr and id of the node asking
        """
        if (self.predecessor_id == None): 
            self.send(args["from"], {"method": "SUCCESSOR_REP", "args": {"req_id": args["id"], "successor_id": self.identification, "successor_addr": self.addr}})

        if contains(self.identification, self.successor_id, args["id"]):
            self.send(args["from"], {"method": "SUCCESSOR_REP", "args": {"req_id": args["id"], "successor_id": self.successor_id, "successor_addr": self.successor_addr}})
        
        else:
            self.send(self.successor_addr, {"method": "SUCCESSOR", "args": {"id": args["id"], "from": args["from"]}})

                
    def notify(self, args):
        """Process NOTIFY message.
            Updates predecessor pointers.

        Parameters:
            args (dict): id and addr of the predecessor node
        """
        if self.predecessor_id is None or contains(
            self.predecessor_id, self.identification, args["predecessor_id"]
        ):
            self.predecessor_id = args["predecessor_id"]
            self.predecessor_addr = args["predecessor_addr"]


    def stabilize(self, from_id, addr):
        """Process STABILIZE protocol.
            Updates all successor pointers.

        Parameters:
            from_id: id of the predecessor of node with address addr
            addr: address of the node sending stabilize message
        """
        if from_id is not None and contains(
            self.identification, self.successor_id, from_id
        ):
            # Update our successor
            self.successor_id = from_id
            self.successor_addr = addr
            self.finger_table.update(1, self.successor_id, self.successor_addr)
        # notify successor of our existence, so it can update its predecessor record
        args = {"predecessor_id": self.identification, "predecessor_addr": self.addr}
        self.send(self.successor_addr, {"method": "NOTIFY", "args": args})

        for entry in self.finger_table.refresh():
            self.send(entry[2], {"method": "SUCCESSOR", "args": {"id": entry[1], "from": self.addr}})


    def sudoku_request(self, sudoku):
        print("Sudoku request: ", sudoku)
        id = dht_hash(sudoku.__str__())

        if id in self.sudokuIds:
            self.sudokuIds.remove(id)
            return "Sudoku already being solved"
        
        stop = threading.Event()
        self.mySudokus[id] = (sudoku, stop)

        self.send(self.successor_addr, {"method": "NEW_Sudoku", "args": {"sudokuID": id, "sudoku": sudoku, "sender": self.addr}})

        stop.wait()

        solved_sudoku = self.mySudokus[id][0]
        del self.mySudokus[id]

        return solved_sudoku
    
    
    # get the stats details (solved and validations)
    def statsDetails(self):
        return self.stats, self.solved
    
    
    # get the network details
    def networkDetails(self):
        self.network = {}
        self.network[self.addr] = [self.predecessor_addr, self.successor_addr]
        self.allNetwork[self.addr] = [self.predecessor_addr, self.successor_addr]
 
        self.send(self.successor_addr, {"method": "NETWORK", "args": self.network, "sender": self.addr})

        self.blocker.clear()
        self.blocker.wait()

        return self.network
    

    # distributed algorithm to solve the sudoku
    def solve_sudoku(self, id, sudoku):
        if id not in self.sudokuIds:
            return
        
        self.validations += 1
        if not sudoku.check():
            for row in range(9):
                for col in range(9):

                    if sudoku.grid[row][col] == 0:
                        for num in range(1, 10):
                            self.validations += 1

                            if sudoku.check_is_valid(row, col, num):
                                sudoku.grid[row][col] = num
                                self.request_Solve(id, sudoku.grid)
                        return
        self.send_solve(id, sudoku.grid)
    

    # ask other nodes to solve the sudoku
    def request_Solve(self, identification, sudoku, timeout=3):
        self.stats[self.addr] = self.validations
        self.lock = 0

        try:
            self.send(self.successor_addr, {"method": "TRY_Solve", "args": {"sudokuID": identification, "sudoku": sudoku}, "validations": self.stats})
        except TimeoutError:
            time.sleep(timeout)
            timeout *= 2
            if (timeout > 100):
                timeout = 3
            self.request_Solve(identification, sudoku, timeout)
        

    def send_solve(self, identification, sudoku, totalSolves=0, addr=None, timeout=3):
        self.stats[self.addr] = self.validations
        if addr is None:
            addr = self.addr

        try:
            self.send(self.successor_addr, {"method": "SOLVED", "args": {"sudokuID": identification, "sudoku": sudoku, "sender": addr}, "validations": self.stats, "solved": totalSolves})
        except TimeoutError:
            time.sleep(timeout)
            timeout *= 2
            if (timeout > 100):
                timeout = 3
            self.send_solve(identification, sudoku, totalSolves, addr, timeout)


    def network_update(self):
        while True:
            threading.Thread(target=self.networkDetails).start()
            time.sleep(HEARTBEAT_INTERVAL)


    def get_network(self):
        while True:
            threading.Thread(target=self.networkDetails).start()
            time.sleep(HEARTBEAT_INTERVAL)


    def send_heartbeat(self):
        while True:
            self.check_heartbeats()
            self.send(self.successor_addr, {"method": "HEARTBEAT", "args": {"sender": self.addr}})
            time.sleep(HEARTBEAT_INTERVAL)


    def receive_heartbeat(self, sender):
        self.last_heartbeat[sender] = time.time()


    def check_heartbeats(self):
        for sender, last_heartbeat in self.last_heartbeat.items():
            if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
                if sender != self.addr:
                    print(f"Node {sender} is dead")

                if sender in self.allNetwork.keys():
                    if self.successor_addr == sender:
                        self.successor_addr = self.allNetwork[sender][1]
                    elif self.predecessor_addr == sender:
                        self.predecessor_addr = self.allNetwork[sender][0]

        
    def track_tries(self, identification):
        self.time_verify[identification] = time.time()


    # verify if the nodes are sending Try_Solve messages (if not, the sudoku is impossible to solve)
    def timeout_verification(self):
        Keys =  []
        for identification, times in self.time_verify.items():
            if time.time() - times > 30:
                
                # verify if the sudoku was requested by the node
                if identification in self.mySudokus.keys():
                    self.mySudokus[identification] = (None, self.mySudokus[identification][1])
                    # unlock the semaphore to indicate that the sudoku is impossible to solve
                    self.mySudokus[identification][1].set()

                Keys.append(identification)

        for key in Keys:
            del self.time_verify[key]       

        threading.Timer(5, self.timeout_verification).start()


    def run(self):
        print(f"Node {self.identification} listening on {self.addr[0]}:{self.addr[1]}")

        # Start threads
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        threading.Thread(target=self.get_network).start()
        threading.Thread(target=self.timeout_verification).start()
        
        # Loop untiln joining the DHT
        while not self.inside_dht:
            join_msg = {
                "method": "JOIN_REQ",
                "args": {"addr": self.addr, "id": self.identification},
            }
            self.send(self.dht_address, join_msg)
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                if output["method"] == "JOIN_REP":
                    args = output["args"]
                    self.successor_id = args["successor_id"]
                    self.successor_addr = args["successor_addr"]
                    self.sudokuIds = output["sudokuIds"]
                    self.finger_table.fill(self.successor_id, self.successor_addr)
                    self.inside_dht = True

        while not self.done:
            payload, addr = self.recv()
            if payload is not None:
                output = pickle.loads(payload)
                if output["method"] == "JOIN_REQ":
                    self.node_join(output['args'])

                # message to check if the node is alive
                elif output["method"] == "HEARTBEAT":
                    self.receive_heartbeat(output["args"]["sender"])

                elif output["method"] == "NOTIFY":
                    self.notify(output["args"])

                elif output["method"] == "PREDECESSOR":
                    # Reply with predecessor id
                    self.send(
                        addr, {"method": "STABILIZE", "args": self.predecessor_id}
                    )

                elif output["method"] == "SUCCESSOR":
                    # Reply with successor of id
                    self.get_successor(output["args"])

                elif output["method"] == "STABILIZE":
                    # Initiate stabilize protocol
                    self.stabilize(output["args"], addr)

                elif output["method"] == "SUCCESSOR_REP":
                    args = output["args"]
                    idx = self.finger_table.getIdxFromId(args["req_id"])
                    self.finger_table.update(idx, args["successor_id"], args["successor_addr"])

                # message to initiate the sudoku solving process
                elif output["method"] == "NEW_Sudoku":
                    args = output["args"]
                    self.sudokuIds.append(args["sudokuID"])

                    if not (args["sender"] == self.addr):
                        self.send(self.successor_addr, {"method": "NEW_Sudoku", "args": {"sudokuID": args["sudokuID"], "sudoku": args["sudoku"], "sender": args["sender"]}})
                    else:
                        threading.Thread(target=self.solve_sudoku, args=(args["sudokuID"], Sudoku(args["sudoku"], self.handicap / 1000))).start()

                # message to try to solve the sudoku
                elif output["method"] == "TRY_Solve":
                    args = output["args"]
                    Dict_stats = output["validations"]
                    self.track_tries(args["sudokuID"])

                    for (key, value) in Dict_stats.items():
                        self.stats[key] = value

                    if (args["sudokuID"] in self.sudokuIds):
                        curSudoku = Sudoku(args["sudoku"], self.handicap / 1000)
                        threading.Thread(target=self.solve_sudoku, args=(args["sudokuID"], curSudoku)).start()

                # message to indicate that the sudoku has been solved
                elif output["method"] == "SOLVED":
                    args = output["args"]
                    if args["sudokuID"] in self.sudokuIds:
                        self.sudokuIds.remove(args["sudokuID"])

                    Dict_stats = output["validations"]
                    self.solved = max(output["solved"], self.solved)

                    for (key, value) in Dict_stats.items():
                        self.stats[key] = value

                    if (args["sudokuID"] in self.mySudokus.keys()):
                        self.mySudokus[args["sudokuID"]] = (args["sudoku"], self.mySudokus[args["sudokuID"]][1])
                        self.mySudokus[args["sudokuID"]][1].set()
                    if not ( args["sender"] == self.addr):
                        self.send_solve(args["sudokuID"], args["sudoku"], addr=args["sender"], totalSolves=self.solved)
                    else:
                        # semaphore to lock the solved variable
                        if self.lock == 0:
                            self.lock = 1
                            self.solved += 1

                        self.send(self.successor_addr, {"method": "UPDATE_SOLVE_COUNT", "solved": self.solved, "sender": self.addr})

                # message to update the solved variable
                elif output["method"] == "UPDATE_SOLVE_COUNT":
                    self.solved = output["solved"]

                    if not (self.addr == output["sender"]):
                        self.send(self.successor_addr, {"method": "UPDATE_SOLVE_COUNT", "solved": self.solved, "sender": output["sender"]})
                
                # message to get the network details
                elif output["method"] == "NETWORK":
                    network_grid = output["args"]
                    network_grid[self.addr] = [self.predecessor_addr, self.successor_addr]

                    for (key, value) in network_grid.items():
                        self.allNetwork[key] = value
                    self.network = network_grid

                    if not (self.addr == output["sender"]):
                        self.send(self.successor_addr, {"method": "NETWORK", "args": network_grid, "sender": output["sender"]})
                    else:
                        self.blocker.set()

            else:  # timeout occurred, lets run the stabilize algorithm
                # Ask successor for predecessor, to start the stabilize process
                self.send(self.successor_addr, {"method": "PREDECESSOR"})


    def __str__(self):
        return "Node ID: {}; DHT: {}; Successor: {}; Predecessor: {}; FingerTable: {}".format(
            self.identification,
            self.inside_dht,
            self.successor_id,
            self.predecessor_id,
            self.finger_table,
        )


    def __repr__(self):
        return self.__str__()