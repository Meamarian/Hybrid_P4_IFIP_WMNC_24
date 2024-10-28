# Hybrid P4 Solution for 5G gNB Data Plane Programmability

## Description
This repository provides a hybrid approach for implementing a 5G gNB (gNodeB) by utilizing both a P4-programmable SmartNIC and x86 server for data plane programmability. The solution leverages the low-latency processing of a SmartNIC for simpler packet tasks, while handling complex processing tasks on an x86 server with DPDK, which is better suited for functions like buffering and retransmission. This decomposition enhances throughput, scalability, and efficiency in 5G networks, enabling optimal use of diverse hardware capabilities.

Our implementation includes:
- **P4 code**: For packet processing on SmartNIC.
- **DPDK code**: For handling tasks on the x86 server.
- **Traffic Generator (TG) Code**: To simulate network traffic for testing.
- **Configuration files**: For setting up the system components and environment.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/your-repository.git
   cd your-repository

