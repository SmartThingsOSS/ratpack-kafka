#!/usr/bin/env python

import argparse
import socket
import time
import sys

def main(timeout, poll, host, port):
	print "Attempting to connect to {}:{} ...".format(host, port)
	wait_time = 0
	while True:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			s.connect((host, port))
			print "Connected successfully after {} seconds".format(wait_time)
			s.close()
			return 0
		except socket.error:
			print "Error connecting after {} seconds".format(wait_time)
		sys.stdout.flush()
		wait_time += poll
		if wait_time <= timeout:
			time.sleep(poll)
		else:
			print "Unable to connect. Exiting."
			return 1

if __name__ == '__main__':
	p = argparse.ArgumentParser()
	p.add_argument('--timeout', default=20, type=int, help='How long to wait until failing (seconds)')
	p.add_argument('--poll', default=2, type=int, help='How often to poll server')
	p.add_argument('--host', default='127.0.0.1', help='Which host to connect to')
	p.add_argument('--port', default=9042, type=int, help='Which port to connect to')
	args = p.parse_args()
	sys.exit(main(args.timeout, args.poll, args.host, args.port))
