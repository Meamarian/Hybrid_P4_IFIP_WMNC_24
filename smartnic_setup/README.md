## Running two smartnic beside each other

1. Go to the system directory and change service mng file as guide, for example:
   ```bash
   cd /lib/systemd/system
   sudo nano nfp-hwdbg-srv2.service

2. Run setup bash script



## Important note

Before load the SmartNIC module on Linux Server, check if the required parameters has specified on blacklist file of modprobe.d directory (/etc/modprobe.d/blacklist-netronome.conf). If not, create the file with this content bellow:

```bash
sudo nano /etc/modprobe.d/blacklist-netronome.conf
   
# This file is used to set the nfp module load parameters
#blacklist the nfp_netvf module
blacklist nfp_netvf

# Disable netdev mode; implies cpp mode is enabled
options nfp nfp_pf_netdev=0
```

If this file does not exists, create then and, after this, run the follow command in order to update initramfs:
```bash
$ sudo update-initramfs -u
