# Hybrid P4 Solution for 5G gNB Data Plane Programmability

## Description
This repository provides a hybrid approach for implementing a 5G gNB (gNodeB) by utilizing both a P4-programmable SmartNIC and x86 server for data plane programmability. The solution leverages the low-latency processing of a SmartNIC for simpler packet tasks, while handling complex processing tasks on an x86 server with DPDK, which is better suited for functions like buffering and retransmission. This decomposition enhances throughput, scalability, and efficiency in 5G networks, enabling optimal use of diverse hardware capabilities.

Our implementation includes:
- **P4 code**: For packet processing on SmartNIC.
- **DPDK code**: For handling tasks on the x86 server.
- **Traffic Generator (TG) Code**: To simulate network traffic for testing.
- **Configuration files**: For setting up the system components and environment.

## Table of Contents
- [Compile](#compile)
- [Run](#run)
- [Trex_RUN](#trexrun)

## Compile
1. Clone this repository:
   ```bash
   git clone https://github.com/your-repository.git
   cd your-repository

## Run


## Test
1. Run the Trex server
   ```bash
   ~/trex-core/scripts$ sudo ./t-rex-64 -i -c 3 --stl --no-scapy-server --hdrh

2. Run the Trex console
   ```bash
   ~/trex-core/scripts$ sudo ./trex-console

3. Run the test_perf_x.py inside the console
   ```bash
   trex>start -f stl/test_perf_1.py -p 0 -d 15 -t --downlink percentage-100 --pktsize 128;start -f stl/test_perf_2.py -p 1 -d 15 -t --downlink percentage-100 --pktsize 128; start -f stl/test_perf_3.py.py -p 2 -d 15 -t --downlink percentage-35 --pktsize 128;
