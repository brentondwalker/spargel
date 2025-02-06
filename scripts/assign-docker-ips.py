#!/usr/bin/env python3

import subprocess
import re
import sys
import json
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", '--start_address', help='starting ip address', type=int)
    parser.add_argument("-n", '--num_addresses', help='number of addresses to assign', type=int)
    parser.add_argument("-p", '--network_prefix', help='the network prefix to assignem them in', default='10.10.1')
    parser.add_argument("-m", '--subnet_mask', help='length of subnet mask', type=int, default=24)
    parser.add_argument("-d", '--delete_old', help='delete teh old network addresses', action='store_true')
    parser.add_argument("-x", '--execute', help='execute the commands', action='store_true')
    parser.add_argument("-c", '--containers', help='start the corresponding containers', action='store_true')
    args = parser.parse_args()

    # get the experimental network interface
    nw_interface_cmd = "ip --br a|grep "+args.network_prefix+"."
    response = subprocess.Popen(nw_interface_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    exp_if = response[0].decode().split()[0]
    #print(exp_if)

    # clear any old ip assignments?

    # add ip addresses
    for aa in range(args.start_address, args.start_address+args.num_addresses):
        ip_addr_cmd = "sudo ip addr add "+args.network_prefix+"."+str(aa)+"/"+str(args.subnet_mask)+" broadcast "+args.network_prefix+".255 dev "+exp_if+" label "+exp_if+":"+str(aa)
        print(ip_addr_cmd)
        if args.execute:
            response = subprocess.Popen(ip_addr_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            print(response)
        if args.containers:
            spark_home = "/opt/spark"
            spark_master = "spark://10.10.1.1:7077"
            docker_cmd = "sudo docker run -d --privileged -v "+spark_home+":/opt/spark --net=host --cpus=1 spark-test-worker --ip "+args.network_prefix+"."+str(aa)+" "+spark_master
            print(docker_cmd)
            docker_response = subprocess.Popen(docker_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            print(docker_response)


    
# ======================================
# ======================================
# ======================================
        
if __name__ == "__main__":
    main()


