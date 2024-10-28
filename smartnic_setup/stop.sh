#!/bin/bash

#Stopping services
echo -n "Stopping services..."
sudo systemctl stop nfp-sdk6-rte
sudo systemctl stop nfp-hwdbg-srv
echo "done"

#unload old firmware
echo -n "Unloading old firmware..."
sudo /opt/netronome/bin/nfp-nffw unload
echo "done"

#kill old server
echo -n "Killing old server..."
sudo pkill pif_rte
echo "done"
