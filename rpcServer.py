from werkzeug.wrappers import Request, Response
from werkzeug.serving import run_simple

import socket
import threading
import pickle
import requests
import json
import os
import subprocess
import time
from multiprocessing import Process

from jsonrpc import JSONRPCResponseManager, dispatcher
threads = []
file = 'config.xml'
from xml.etree import ElementTree
dom = ElementTree.parse(file)
root = dom.getroot()
total_ips = list()
for i in root.find('RPC_Server_IPs'):
	total_ips.append(i.text)


def word_count(filename, offset, length):
	log_file = open(filename, "r", encoding="utf8")
	lines = log_file.readlines()
	file_segment = lines[offset:offset+length]
	file_segment2 = [i.split() for i in file_segment]
	file_segment3 = list()
	for i in file_segment2:
	    file_segment3.extend(i)
	word_count = len(file_segment3)
	return  word_count

def word_instances(filename, offset, length, word):
	log_file = open(filename, "r",encoding="utf8")
	lines = log_file.readlines()
	file_segment = lines[offset:offset+length]
	file_segment2 = [i.split() for i in file_segment]
	file_segment3 = list()
	for i in file_segment2:
	    file_segment3.extend(i)
	word_count = len(file_segment3)
	word_instances = file_segment3.count(word)
	return word_instances


@dispatcher.add_method
def foobar(**kwargs):
    return kwargs["foo"] + kwargs["bar"]


@Request.application
def application(request):
    # Dispatcher is dictionary {<method_name>: callable}
    dispatcher["word_count"] = lambda filename, offset, length : word_count(filename, offset, length)
    dispatcher["word_instances"] = lambda filename, offset, length, word : word_instances(filename, offset, length, word)

    response = JSONRPCResponseManager.handle(
        request.data, dispatcher)
    return Response(response.json, mimetype='application/json')

def rpc_caller(ip,port,application):
	run_simple(ip,port,application)

processes = []
if __name__ == '__main__': 
	for i in range(0,len(total_ips)):
	   p = Process(target=rpc_caller,args=(total_ips[i],4000,application))
	   p.start()
	   processes.append(p)

	for p in processes:
	   p.join()
