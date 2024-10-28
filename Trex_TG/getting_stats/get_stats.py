import sys
sys.path.append('/home/dv/mohsen/trex-core/scripts/automation/trex_control_plane/interactive') #/trex/examples/stl/

#from trex_stl_lib.api import *
import argparse
#from mpdccp import *
from scapy.all import *
from scapy.utils import PcapWriter
from pprint import pprint
# Example showing how to define stream for latency measurement, and how to parse the latency information
#import stl_path
from trex.stl.api import *
import time
import pprint

def rx_example (tx_port, rx_port):

    # create client
    c = STLClient()
    passed = True
    
    try:

        # connect to server
        c.connect()

        rc = rx_iteration(c, tx_port, rx_port)
        if not rc:
            passed = False

    except STLError as e:
        passed = False
        print(e)

    finally:
        c.disconnect()

    if passed:
        print("\nTest passed :-)\n")
    else:
        print("\nTest failed :-(\n")

# RX one iteration
def rx_iteration (c, tx_port, rx_port):
    
    pgids = c.get_active_pgids()
    print ("Currently used pgids: {0}".format(pgids))

    # for i in range(1,8):
    #     time.sleep(1)
    #     stats = c.get_pgid_stats(pgids['latency'])
    #     flow_stats = stats['flow_stats'].get(5)
    #     print(flow_stats)
    #     rx_pps = flow_stats['rx_pps'][rx_port]
    #     tx_pps = flow_stats['tx_pps'][tx_port]
    #     rx_bps = flow_stats['rx_bps'][rx_port]
    #     tx_bps = flow_stats['tx_bps'][tx_port]
    #     rx_bps_l1 = flow_stats['rx_bps_l1'][rx_port]
    #     tx_bps_l1 = flow_stats['tx_bps_l1'][tx_port]
    #     print("rx_pps:{0} tx_pps:{1}, rx_bps:{2}/{3} tx_bps:{4}/{5}"
    #           .format(rx_pps, tx_pps, rx_bps, rx_bps_l1, tx_bps, tx_bps_l1))
    #c.wait_on_traffic(ports = [tx_port])
    w=0
    while w==0: 
        time.sleep(2)
        stats = c.get_pgid_stats(pgids['latency'])
        pprint.pprint(stats)
    flow_stats = stats['flow_stats'].get(5)
    global_lat_stats = stats['latency']
    lat_stats = global_lat_stats.get(5)
    if not flow_stats:
        print("no flow stats available")
        return False
    if not lat_stats:
        print("no latency stats available")
        return False

    tx_pkts  = flow_stats['tx_pkts'].get(tx_port, 0)
    tx_bytes = flow_stats['tx_bytes'].get(tx_port, 0)
    rx_pkts  = flow_stats['rx_pkts'].get(rx_port, 0)
    drops = lat_stats['err_cntrs']['dropped']
    ooo = lat_stats['err_cntrs']['out_of_order']
    dup = lat_stats['err_cntrs']['dup']
    sth = lat_stats['err_cntrs']['seq_too_high']
    stl = lat_stats['err_cntrs']['seq_too_low']
    old_flow = global_lat_stats['global']['old_flow']
    bad_hdr = global_lat_stats['global']['bad_hdr']
    lat = lat_stats['latency']
    jitter = lat['jitter']
    avg = lat['average']
    tot_max = lat['total_max']
    tot_min = lat['total_min']
    last_max = lat['last_max']
    hist = lat ['histogram']
    
    if c.get_warnings():
            print("\n\n*** test had warnings ****\n\n")
            for w in c.get_warnings():
                print(w)
            return False

    print('Error counters: dropped:{0}, ooo:{1} dup:{2} seq too high:{3} seq too low:{4}'.format(drops, ooo, dup, sth, stl))
    if old_flow:
        print ('Packets arriving too late after flow stopped: {0}'.format(old_flow))
    if bad_hdr:
        print ('Latency packets with corrupted info: {0}'.format(bad_hdr))
    print('Latency info:')
    print("  Maximum latency(usec): {0}".format(tot_max))
    print("  Minimum latency(usec): {0}".format(tot_min))
    print("  Maximum latency in last sampling period (usec): {0}".format(last_max))
    print("  Average latency(usec): {0}".format(avg))
    print("  Jitter(usec): {0}".format(jitter))
    print("  Latency distribution histogram:")
    l = list(hist.keys()) # need to listify in order to be able to sort them.
    l.sort()
    for sample in l:
        range_start = sample
        if range_start == 0:
            range_end = 10
        else:
            range_end  = range_start + pow(10, (len(str(range_start))-1))
        val = hist[sample]
        print ("    Packets with latency between {0} and {1}:{2} ".format(range_start, range_end, val))

    return True

# run the tests
rx_example(tx_port = 0, rx_port = 0)
