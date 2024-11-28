## Description

The program clones the packet; one copy goes to the VF and is processed by l2fwd with DPDK, while the other copy is sent to the output and to TRex. If we want to receive both the return-clone and the original packet on the same PF, the total throughput is 8 MPPS. However, if we configure the MAC addresses so that the return-clone is received on another port, the total throughput increases to 16 MPPS, with a sending rate of 8 MPPS.

This program is designed for return-clone configuration. It sets up the Open5G-TRex server's MAC address, allowing TRex to recognize it. 
The type of clone is E2, sending clones to VF code section is on egress control flow.
