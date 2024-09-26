import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor

class HttpServer(BaseHTTPRequestHandler):
    validations_count = 0

    def do_GET(self):
        if self.path == '/stats':
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.node.statsDetails)
                stats, solved = future.result() #dictionary with the stats of each node

                HttpServer.validations_count = 0
                # calculate the total number of solved sudokus and validations
                for peer in stats:
                    HttpServer.validations_count += stats[peer]

            stats = {
                "all": {
                    "solved": solved,
                    "validations": HttpServer.validations_count,
                },
                "nodes": [
                    {
                        "address": peer[0] + ":" + str(peer[1]),
                        "validations": stats[peer],  
                    } for peer in stats
                ]
            }
            
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(bytes(json.dumps(stats), "utf-8"))
        
        elif self.path == '/network':
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.node.networkDetails)
                nodes = future.result() # dictionary with the nodes and their connections

            network = {node[0] + ":" + str(node[1]): [nodes[node][i][0] + ":" + str(nodes[node][i][1]) for i in range(2) if nodes[node][i] is not None] for node in nodes}

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(bytes(json.dumps(network), "utf-8"))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data)

        if self.path == '/solve': 
            if "sudoku" in data:
                sudoku = data["sudoku"]

                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(self.node.sudoku_request, sudoku)
                    solved_sudoku = future.result()

                    if solved_sudoku == None:
                        self.send_response(400)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        response = bytes(json.dumps({"error": "Error! Invalid sudoku!"}), "utf-8")
                        self.wfile.write(response)
                        return

                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response = bytes(json.dumps(solved_sudoku), "utf-8")
                self.wfile.write(response)

            else:
                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(bytes(json.dumps({"error": "No sudoku provided"}), "utf-8"))

        else:
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(bytes(json.dumps({"error": "Invalid path"}), "utf-8"))


def factory(node):
    class CustomHandler(HttpServer):
        def __init__(self, *args, **kwargs):
            self.node = node
            super().__init__(*args, **kwargs)
    return CustomHandler


def run_server(node, ip='localhost', server_class=ThreadingHTTPServer, port=8080):
    server_address = (ip, port)
    handler_class = factory(node)
    httpd = server_class(server_address, handler_class)
    print(f'Starting http on {ip}:{port}')
    httpd.serve_forever()