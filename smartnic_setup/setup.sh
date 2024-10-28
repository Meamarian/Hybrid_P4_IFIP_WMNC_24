#!/bin/bash

#unload firmware
echo "Unloading old firmware..."
#sudo /opt/netronome/bin/nfp-nffw unload
sudo /opt/netronome/bin/nfp-nffw unload -Z "67:00.0"
sudo /opt/netronome/bin/nfp-nffw unload -Z "b3:00.0"

#kill server
echo "Killing old server..."
sudo pkill pif_rte

#stop services
echo "Stopping services..."
sudo systemctl stop nfp-sdk6-rte
sudo systemctl stop nfp-hwdbg-srv
sudo systemctl stop nfp-sdk6-rte2
sudo systemctl stop nfp-hwdbg-srv2

#reload kernel modules
echo "Restarting kernel modules..."
sudo depmod
sudo modprobe -r nfp #remove nfp kernel module

BIND_TO_PF=$1

if [ $BIND_TO_PF -eq 1 ]
then
        echo "Enabling nfp_pf_netdev"
        sudo modprobe nfp nfp_pf_netdev=1
else
        sudo modprobe nfp
fi

#sudo modprobe nfp
#sudo modprobe nfp nfp_pf_netdev=1
#sudo modprobe nfp nfp_dev_cpp=1
#sudo modprobe nfp nfp_dev_cpp=1 nfp_pf_netdev=1 #load with debugging support
sudo modprobe devlink

#restart services
echo "Starting services..."
sudo systemctl restart nfp-sdk6-rte
sudo systemctl restart nfp-hwdbg-srv
sudo systemctl restart nfp-sdk6-rte2
sudo systemctl restart nfp-hwdbg-srv2
