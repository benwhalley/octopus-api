#!/usr/bin/env python

# Reflects the requests from HTTP methods GET, POST, PUT, and DELETE
# Written by Nathan Hamiel (2010)

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from optparse import OptionParser
from urllib.parse import parse_qs
import random


class RequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        request_path = self.path
        request_headers = self.headers
        content_length = request_headers.get("Content-Length")
        length = int(content_length) if content_length else 0
        request_payload = self.rfile.read(length)
        # Parse the form data into a dictionary
        form_data = parse_qs(request_payload.decode("utf-8"))
        print(request_path)
        # Send response with the form data as JSON
        # randomly send status code 429 to simulate rate limiting (1 in 6)

        self.send_response(random.choice([200] * 5 + [429]))
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(form_data).encode("utf-8"))

    do_PUT = do_POST


def main():
    port = 8001
    print("Listening on 0.0.0.0:%s" % port)
    server = HTTPServer(("", port), RequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    parser = OptionParser()
    parser.usage = "Creates an http-server that will echo out any POST parameters\n"
    (options, args) = parser.parse_args()
    main()


#         # Get the content length

#         # Read the request payload
#         request_payload = self.rfile.read(length)
#         print("Request payload:", request_payload)

#         # Parse the form data into a dictionary
#         form_data = parse_qs(request_payload.decode('utf-8'))

#         print("<----- Request End -----\n")

#         # Send response with the form data as JSON
#         self.send_response(random.choice([200]*5+[429]))
#         self.send_header('Content-type', 'application/json')
#         self.end_headers()
#         self.wfile.write(json.dumps(form_data).encode('utf-8'))
#     do_PUT = do_POST

# def main():
#     port = 8001
#     print('Listening on 0.0.0.0:%s' % port)
#     server = HTTPServer(('', port), RequestHandler)
#     server.serve_forever()

# if __name__ == "__main__":
#     parser = OptionParser()
#     parser.usage = ("Creates an http-server that will echo out any GET or POST parameters\n"
#                     "Run:\n\n"
#                     " reflect")
#     (options, args) = parser.parse_args()
#     main()
