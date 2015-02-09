"""
Simple password-protected web server to serve the results
"""

from SimpleHTTPServer import SimpleHTTPRequestHandler
import SocketServer

import sys
import base64
from time import sleep

from configs import web_config

try:
    PORT = int(web_config['port'])
    PASSWORD = base64.b64encode("%s:%s" % (web_config['user'], web_config['password']))
except ValueError:
    print "Please supply a correct port number"
    sys.exit(1)

class CustomHandler(SimpleHTTPRequestHandler):

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        if self.headers.getheader('Authorization') == "Basic " + PASSWORD:
            SimpleHTTPRequestHandler.do_GET(self)
        else:
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="Yoka"')
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write("Sorry, not authorized.")
            sleep(1)


httpd = SocketServer.TCPServer(("", PORT), CustomHandler)

print "serving at port", PORT
try:
    httpd.serve_forever()
finally:
    httpd.shutdown()