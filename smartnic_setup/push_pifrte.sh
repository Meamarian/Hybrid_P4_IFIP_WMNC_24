#!/bin/bash

PORT=$1
case $PORT in
    20206)
        appendix= 
    ;;
    20207)
        appendix=1
    ;;
    20208)
        appendix=2
    ;;
    20209)
        appendix=3
    ;;
    20210)
        appendix=4
    ;;
esac



DIR=$(pwd)
LOG_FILE_RUN=$DIR/out_load #outputfromthisrun
LOG_FILE_MAIN=/var/log/nfp-sdk6-rte.log
FIRMWARE_FILE=$DIR/p4/gnb/gnb.nffw #pathtofirmware
DESIGN_FILE=$DIR/p4/gnb/out/pif_design.json #pathtodesign
CONFIG_FILE=$DIR/p4/gnb/config.p4cfg #pathtoconfig
#restartservices
echo stopping services
sudo systemctl stop nfp-sdk6-rte$appendix
sudo systemctl stop nfp-hwdbg-srv$appendix
#unloadoldfirmware
echo unloading formware
sudo /opt/netronome/bin/nfp-nffw unload
#sudo pkill pif_rte
#loadfirmwareandrulestocard
echo loading firmware
#pushd /opt/nfp_pif/bin/ > /dev/null
cd /opt/nfp_pif/bin/
#sudo ./pif_rte -h
sudo ./pif_rte -v INFO -n $(($PORT-20206)) -p $PORT -I -s /opt/nfp_pif/scripts/pif_ctl_nfd.sh -f $FIRMWARE_FILE -d $DESIGN_FILE -c $CONFIG_FILE --log_file $LOG_FILE_MAIN > $LOG_FILE_RUN
#popd > /dev/null
