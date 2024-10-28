# Utilizing Hybrid P4 Solutions to Enhance 5G gNB with Data Plane Programmability

## Description
<p align="justify">
This GitHub repository contains the implementation details, code, and configurations for the paper titled **"Utilizing Hybrid P4 Solutions to Enhance 5G gNB with Data Plane Programmability,"** which has been accepted for presentation at the **15th IFIP Wireless and Mobile Networking Conference (IFIP WMNC 2024)**, to be held in Venice on **November 11-12, 2024**.

The paper provides a hybrid approach for implementing a 5G gNB (gNodeB) by utilizing both a P4-programmable SmartNIC and x86 server for data plane programmability. The solution leverages the low-latency processing of a SmartNIC for simpler packet tasks, while handling complex processing tasks on an x86 server with DPDK, which is better suited for functions like buffering and retransmission. This decomposition enhances throughput, scalability, and efficiency in 5G networks, enabling optimal use of diverse hardware capabilities.
</p>

Our implementation includes:
- **P4 code**: For packet processing on SmartNIC.
- **DPDK code**: For handling tasks on the x86 server.
- **Traffic Generator (TG) Code**: To simulate network traffic for testing.
- **Configuration files**: For setting up the system components and environment.


## Authors
**Mohsen Memarian<sup>1</sup>, Andreas Kassler<sup>1,2</sup>, Karl-Johan Grinnemo<sup>1</sup>, Sándor Laki<sup>3</sup>, Gergely Pongracz<sup>4</sup>, Johan Forsman<sup>5</sup>**

<sup>1</sup>Karlstad University,  
<sup>2</sup>Deggendorf Institute of Technology,  
<sup>3</sup>ELTE Eötvös Loránd University,  
<sup>4</sup>Ericsson Research,  
<sup>5</sup>TietoEvry

## Table of Contents
- [Compile](#compile)
- [Run](#run)
- [Trex_RUN](#trexrun)

## Compile
1. Clone this repository:
   ```bash
   https://github.com/Meamarian/Hybrid_P4_IFIP_WMNC_24.git

2. Go to the dpdk app directory
   ```bash
   cd dpdk/examples/gnb/

3. Compile the dpdk gNB app
   ```bash
   sudo mkdir build
   sudo make

## Run
1. Run gNB app as primary app
   ```bash
   sudo ./gnb run   -w 0000:67:08.0 -w 0000:67:08.1 -w 0000:67:08.2 -w 0000:67:08.3 -w 0000:67:08.4 -w 0000:67:08.5   -w 0000:67:08.6 -w 0000:67:08.7 -l 11-18  --proc-type primary --file-prefix=2 -m 3000 -- -p 0xff -q 4

2. Run BaaS app as secondary app
   ```bash
   sudo ./baas -l 13-15 --proc-type secondary --file-prefix=1
   
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
