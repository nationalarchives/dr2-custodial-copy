import socketserver
from http.server import BaseHTTPRequestHandler
import json
from urllib.parse import urlparse

PORT = 3000

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.check_path("/v1/security/login")
        self.send_json({"response": "atoken"})
        return


    def do_GET(self):
        if not self.headers.get("Authorization"):
            self.send_error(401)
        self.check_path("/v1/file")
        body = {
            "archdone": True,
            "copies": [
                {"copy": "1"},
                {"copy": "2"},
                {"copy": "3", "sections": [{"volume": "L03721"}]}
            ]
        }
        self.send_json(body)


    def check_path(self, expected_path):
        parsed = urlparse(self.path)
        if parsed.path != expected_path:
            self.send_error(404)

    def send_json(self, body):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode("utf-8"))


with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print("serving at port", PORT)
    httpd.serve_forever()
