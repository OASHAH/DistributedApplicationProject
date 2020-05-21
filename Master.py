import socket
import time
import pickle
import threading
import requests
import json
import random
import numpy as np
import logging
from pathlib import Path
import os
my_file = Path("output_files")
if not my_file.is_dir():
	os.mkdir("output_files")

with open('master.log', 'w'):
    pass
logging.basicConfig(filename='master.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')
host = '127.0.0.1' #In case i also add user that will communicate with the master.
port = 12345
file = 'config.xml'
from xml.etree import ElementTree
dom = ElementTree.parse(file)
root = dom.getroot()
total_ips = list()
for i in root.find('Worker_IPs'):
	total_ips.append(i.text)
chunk_offsets = list()
for i in root.find('chunk_offsets'):
	chunk_offsets.append(int(i.text))
#chunk_length = int(root.find('chunk_length').text)
chunk_length = list()
for i in root.find('chunk_lengths'):
	chunk_length.append(int(i.text))
total_chunks = len(chunk_offsets)
heartbeat_interval = int(root.find('heartbeat_interval').text)
timeout_interval = heartbeat_interval * int(root.find('timeout_interval').text)
max_heartbeats = int(root.find('max_heartbeats').text)
heartbeat_ips = list()
for i in root.find("HeartBeat_IPs"):
	heartbeat_ips.append(i.text)

#Let n be the workers
#I will create 2n threads that will just send the instruction to n workers.
#n threads will delegate function calls to workers.
#The other threads that will routinely send heartbeat messages.

threads = []
total_workers = total_chunks


#Functions for creating dictionary/json for rpc calls to worker.
def create_parameter(worker_index):
	global filename,chunk_offsets,chunk_length,wrds
	p = [filename,chunk_offsets[worker_index], chunk_length[worker_index],wrds[worker_index]]
	return p

def create_payload(method, parameters): #used to create dict that is json compatible.
	payload = {
		"method": method,
		"params": parameters,
		"jsonrpc": "2.0",
		"id": 0,
	}
	return payload

def request_word_count(s,parameters):
	method = "word_count"
	payload = create_payload(method, parameters)
	msg = pickle.dumps(payload)
	try:
		logging.debug("Function called: word instance by socket" +str(s)+"with parameters"+parameters)
		s.send(msg)
	except:
		print("connection failed")

def request_word_instance(s,parameters):
	method = "word_instances"
	payload = create_payload(method, parameters)
	msg = pickle.dumps(payload)
	try:
		logging.debug("Function: check_assigned_work "+",parameters:["+ str(s)+","+str(parameters)+"]")
		s.send(msg)
	except:

		logging.debug("Function called failed: word instance by socket" +str(s)+"with parameters"+parameters)
		print("connection failed")
#-----------------------------------------------

#All the global variables needed for inter-thread communication.
work_sockets = [] #Global Variable for creating the sockets.
flag = 1 #it will becocme 0 (by worker who finished last), which will imply that work is completed.
finished_workers = 0 #When finished workers == total_work n, flag will become 0.
def get_sockets(total_workers):
	global work_sockets
	global port
	for i in range(0,total_workers):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((total_ips[i], port))
		work_sockets.append(s)
get_sockets(total_workers) #These sockets are used for assigning work.
wrkr_ids = [i for i in range(0,total_workers)] #ids of workers
work_ids = [i for i in range(0,total_workers)] #id of work segments to be assigned
work_completed = [0 for i in range(0,total_workers)]
is_worker_dead = [0 for i in range(0,total_workers)] #0 means not dead, 1 means dead and might need to assign work,
# 2 means dead worker is dealt with and we don't need to consider it again.
is_worker_straggling = [0 for i in range(0, total_workers)]
worker_assigned = [] #defined the work_ids allocated to the worker. -1 if the worker is free.
wrds = ["or","and","the","is","if"]
filename = "enwik8.txt"
#--------------------------------------------------------------------


#Managing Workers and scheduling work.
reassigning_threads = []
def check_assigned_work(index,worker_type): #index of the worker, the type of issue worker is facing
	global worker_assigned, work_completed,reassigning_threads
	#index = index of the dead worker.
	logging.debug("Function: check_assigned_work "+",parameters:["+ str(index)+","+str(worker_type)+"]")
	
	work_id = worker_assigned[index] # the work assigned to this dead worker
	if work_completed[work_id] == 0:
		print("Need to reassign work id: ", work_id)
		t = threading.Thread(target=reassign_work,args=(work_id,))
		t.start()
		reassigning_threads.append(t)
	else:
		logging.debug("Worker"+str(index) + " dead but the work assigned was already completed")
		print("Worker"+str(index) + " dead but the work assigned was already completed")

def reassign_work(work_id):
	global total_workers,worker_assigned,work_sockets,total_workers,work_completed
	flag2 = 1
	logging.debug("Function: reassing_work "+",parameters:["+ str(work_id)+"]")
	
	shuffled_workers = [] #shuffling workers so we don't overload some workers.
	k = random.randint(0,total_workers)
	for i in range(0,5):
		s = (k + i) % 5
		shuffled_workers.append(s)

	while flag2:
		for i in range(0, total_workers):
			j = shuffled_workers[i]
			if (worker_assigned[j] == -1 and is_worker_dead[j] == 0 and is_worker_straggling[j] == 0): #The worker is free and alive.
				worker_assigned[j] = work_id
				#reassign this worker.
				print("Work reassigned to worker",j+1)

				logging.debug("Work: "+str(work_id)+" reassinged to "+str(j+1))
				p = create_parameter(work_id)
				request_word_instance(work_sockets[j],p)
				flag2 = 0
				break
				
def manage_workers_thread():
	global total_workers,work_ids,work_completed,is_worker_dead,worker_assigned,is_worker_straggling
	f = 1
	while True:
		if (sum(work_completed) == total_workers):
			if f == 1:
				f = 0
				print("Work Completed")

		
		for i in range(0,total_workers):
			if (is_worker_dead[i] == 1):
				is_worker_dead[i] = 2
				check_assigned_work(i,0) #this worker is dead
				break

		for i in range(0,total_workers):
			if (is_worker_straggling[i] == 1):
				is_worker_straggling[i] = 0
				check_assigned_work(i,1) #this worker is straggling
				break

	while True:
		pass
	return
#-------------------------------------------------------------------

def main_worker_thread(worker_index,s,host,parameters1,parameters2):
	global finished_workers,work_completed,worker_assigned
	global flag

	request_word_instance(s,parameters2)
	while True:
		try:
			message = s.recv(1024)
			msg = pickle.loads(message)
			if (msg == "work_completed"):
				work_id = worker_assigned[worker_index]
				work_completed[work_id] = 1
				worker_assigned[worker_index] = -1
		except:
			#print("connection to ", host, " died.")
			#s.close()
			#break
			pass
			
def heartbeat_thread(worker_index,host):
	global finished_workers,port,max_heartbeats,is_worker_dead

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((host, port))

	counter = 0
	straggler_counter = 0

	while True:
		
		if (counter >= max_heartbeats):
			is_worker_dead[worker_index] = 1
			print("Worker", worker_index+1, "died")
			logging.debug("Worker "+str(worker_index+1)+" died")
			s.close()
			break

		if (straggler_counter >= max_heartbeats):
			straggler_counter = 0
			if is_worker_dead[worker_index] == 0: #worker is not dead
				if work_completed[worker_index] == 0: #the work of this worker is not completed
					if is_worker_straggling[worker_index] == 0: #this worker was not previously straggling, it is now.
						is_worker_straggling[worker_index] = 1
						logging.debug("Worker "+str(worker_index+1)+" is straggling")
						print("Worker", worker_index+1, "is straggling")

		message = "master_ping"
		time.sleep(heartbeat_interval)
		try:
			msg = pickle.dumps(message)
			s.send(msg)
		except:
			#is_worker_dead[worker_index] = 1
			#print("connection to worker", worker_index, " died.")
			#s.close()
			pass
			#continue
			#pass
		try:
			message = s.recv(1024)
			msg = pickle.loads(message)
			if (msg == "i_am_alive"):
				counter = 0
				straggler_counter += 1
				logging.debug(msg+" recieved from worker"+ str(worker_index+1))
				print(msg, " recieved from worker", worker_index+1)
		except:
			logging.debug("connection to worker"+str(worker_index)+" died.")
			print("connection to worker", worker_index+1, " died.")
			is_worker_dead[worker_index] = 1
			s.close()
			break

		counter+=1

	while True:
		pass
#---------------------------------------------------------

def master(worker_index,work_socket,host,heartbeat_host, port,offset,length,wrd,filename):
	logging.debug("Function = master, "+"Parameters = ["+str(worker_index)+","+str(work_socket)+","+
		str(host)+","+str(heartbeat_host)+","+str(port)+","+str(offset)+","+str(length)
		+","+str(wrd)+","+str(filename)+"]")
	parameters1 = [filename,chunk_offsets[worker_index], chunk_length[worker_index],wrds[worker_index]]
	parameters2 = [filename, chunk_offsets[worker_index], chunk_length[worker_index],wrds[worker_index]]
	hb_thread = threading.Thread(target=heartbeat_thread,args=(worker_index,heartbeat_host,))
	hb_thread.start()
	mw_thread = threading.Thread(target=main_worker_thread,args=(worker_index,
		work_socket,host,parameters1,parameters2))
	mw_thread.start()

	hb_thread.join()
	mw_thread.join()


def main():
	global threads,work_sockets,wrds,filename
	global total_workers,work_ids,work_completed,is_worker_dead,worker_assigned
	global wrkr_ids

	worker_assigned = [i for i in range(0,total_workers)]
	for i in range(0,total_workers):
		#i is worker_id, used for assigning work
		t = threading.Thread(target=master,args=(wrkr_ids[i],work_sockets[i],total_ips[i],heartbeat_ips[i],port,chunk_offsets[i],
			chunk_length[i],wrds[i],filename))
		t.start()
		threads.append(t)

	work_manager_thread = threading.Thread(target=manage_workers_thread)
	work_manager_thread.start()


	for t in threads:
		t.join()
	work_manager_thread.join()

if __name__ == '__main__':
		main()

