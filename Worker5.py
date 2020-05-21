import socket
import threading
import pickle
import requests
import json
import os
import subprocess
import time
from multiprocessing import Process
import logging
logging.basicConfig(filename='workers.log', level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')



#This worker is supposed to be killed.
#TODO:

port = 12345
threads = []
url = "http://localhost:4000/jsonrpc"
file = 'config.xml'
from xml.etree import ElementTree
dom = ElementTree.parse(file)
root = dom.getroot()
total_ips = list()
for i in root.find('Worker_IPs'):
    total_ips.append(i.text)
heartbeat_ips = list()
for i in root.find("HeartBeat_IPs"):
    heartbeat_ips.append(i.text)


def write_to_file(worker_instance, function, result, parameters):
    line1 = worker_instance + ", rpc function:" + \
        function + ", result:" + str(result) + "\n"
    line2 = worker_instance + ": offset/length" + str(parameters) + "\n"
    f = open("output_files/" + worker_instance + ".txt", "w")
    f.write(line1)
    f.write(line2)
    f.close()


def service_master_request(conn, data, worker_instance, host):
    #global url
    url = "http://" + host + ":4000/jsonrpc"
    rpc_function_called = data["method"]
    response = requests.post(url, json=data).json()
    print("Work completed by", worker_instance)
    message = "work_completed"
    msg = pickle.dumps(message)
    logging.debug(worker_instance+", func:"+str(rpc_function_called)+", result: "+str(response["result"]))
    logging.debug(worker_instance+", offset/length: "+str(data["params"][1:3]))
    print(worker_instance, ", func:", rpc_function_called,"result:", response["result"])
    print(worker_instance, "offset/length", data["params"][1:3])
    write_to_file(worker_instance, rpc_function_called,
                  response["result"], data["params"][1:3])
    try:
        conn.send(msg)
        return 1
    except:
        print("connection died")
        return 0


#rpc_threads = []
def new_connection(conn, worker_instance, host):
    global url
    global rpc_threads

    while True:
        try:
            msg = conn.recv(1024)
        except:
            print("connection died")
            break
        data = pickle.loads(msg)

        # if data is str, i recieved a heartbeat message, else master requested an rpc call.
        if isinstance(data, str):
            message = "i_am_alive"
            message1 = pickle.dumps(message)
            try:
                conn.send(message1)
            except:
                print("Connection died")
                break
        elif isinstance(data, dict):
            #t = threading.Thread(target=service_master_request, args=(conn,data,worker_instance,))
            # rpc_threads.append(t)
            time.sleep(5)
            request_completed = service_master_request(
                conn, data, worker_instance, host)
            #break
            # if (request_completed == 0):
            # break


def accept_connection(host, port, worker_instance):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(5)
    threads = []

    while True:
        conn, address = s.accept()
        t = threading.Thread(target=new_connection,
                             args=(conn, worker_instance, host))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


hb_threads = []  # heartbeat threads
w_threads = []  # worker threads


# I am creating 5 worker processes, each with 2 primary threads (in main_worker).
# One thread will check for heartbeat messages, the other will focus on initiating rpc calls
# Therefore, we have 2 sockets for each process, overall ten sockets.

def main_worker(ip, heartbeat_ip, port, worker_instance):
    global hb_threads
    global w_threads

    t = threading.Thread(target=accept_connection,
                         args=(ip, port, worker_instance,))
    t.start()
    w_threads.append(t)
    t = threading.Thread(target=accept_connection, args=(
        heartbeat_ip, port, worker_instance,))
    t.start()
    hb_threads.append(t)

    for i in hb_threads:
        i.join()
    for i in w_threads:
        i.join()


processes = []
if __name__ == '__main__':
    worker_instance = "worker" + str(5)
    p = Process(target=main_worker, args=(
        total_ips[4], heartbeat_ips[4], port, worker_instance))
    p.start()
    processes.append(p)

    for p in processes:
        p.join()
