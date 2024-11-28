## Description

program clone the packet, one goes to VF and l2fwd dpdk, another goes to output and trex. if we want to recieve return-clone and original packet on same PF
total throu is 8 MPPS, but if we config the macs in a way that return-clone goes on another port, total thru is 16 MPPS, with sending rate 8 MPPS.
