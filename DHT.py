import argparse
import socket
from DHT_NODE import DHTNode
import threading
from server import run_server

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def main(ip, http_port, p2p_port, anchor, timeout, handicap):
    """ Script to launch several DHT nodes. """
    # list with all the nodes
    dht = []

    if anchor:
        anchor_host, anchor_port = anchor.split(':')
        anchor_port = int(anchor_port)

        node = DHTNode((ip, p2p_port), (anchor_host, anchor_port), timeout, handicap=handicap)
        dht.append(node)
        
    else:
        # initial node on DHT
        node = DHTNode((ip, p2p_port), handicap=handicap)
        dht.append(node)

    node_tread = threading.Thread(target=node.run)
    node_tread.start()

    server_thread = threading.Thread(target=run_server(node, ip=ip, port=http_port))
    server_thread.start()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Run a P2P node.')
    parser.add_argument('-p', '--http-port', type=int, required=True, help='HTTP port to run the node')
    parser.add_argument('-s', '--p2p-port', type=int, required=True, help='P2P port to run the node')
    parser.add_argument('-a', '--anchor', type=str, help='Address of an anchor node to join the network (format: host:port)')
    parser.add_argument('-H', '--handicap', type=int, default=0, help='Handicap/delay for validation function check (default: 0)')

    args = parser.parse_args()
    ip = get_local_ip()
    http_port = args.http_port
    p2p_port = args.p2p_port
    anchor = args.anchor
    handicap = args.handicap

    main(ip, http_port, p2p_port, anchor, 3, handicap)
