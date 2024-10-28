from trex_stl_lib.api import *
import argparse
#from mpdccp import *
from scapy.all import *
from scapy.utils import PcapWriter

class EthLayer(Packet):
    name = "EthLayer"
    fields_desc = [
        BitField("dmac", 0, 48),
        BitField("smac", 0, 48),
        BitField("type", 0, 16)
    ]

class IPv4Layer(Packet):
    name = "IPv4Layer"
    fields_desc = [
        BitField("version", 0, 4),
        BitField("ihl", 0, 4),
        BitField("diffserv", 0, 8),
        BitField("totalLen", 0, 16),
        BitField("identification", 0, 16),
        BitField("flags", 0, 3),
        BitField("fragOffset", 0, 13),
        BitField("ttl", 0, 8),
        BitField("protocol", 0, 8),
        BitField("hdrChecksum", 0, 16),
        IPField("srcAddr", 0),
        IPField("dstAddr", 0)
    ]


class UDPLayer(Packet):
    name = "UDPLayer"
    fields_desc = [
        BitField("srcPort", 0, 16),
        BitField("dstPort", 0, 16),
        BitField("len", 0, 16),
        BitField("checksum", 0, 16)
    ]

class RLCackmodeLayer(Packet):
    name = "RLCackmodeLayer"
    fields_desc = [
        BitField("dc", 0, 1),
        BitField("p", 0, 1),
        BitField("si", 0, 2),
        BitField("r", 0, 2),
        BitField("snpadding", 0, 2),
        BitField("sn", 0, 16),
        BitField("teid", 0, 32),
		#BitField("rbnumber", 0, 3)
    ]
	
class RLCStatusLayer(Packet):
    name = "RLCStatusLayer"
    fields_desc = [
        BitField("dc", 0, 1),
        BitField("cpd", 0, 3),
        BitField("snpadding", 0, 4),
        BitField("sn", 0, 16),
        BitField("e", 0, 1),
        BitField("r", 0, 7),
        BitField("teid", 0, 32),
		#BitField("rbnumber", 0, 3)
    ]

class RLCNACKLayer(Packet):
    name = "RLCNACKLayer"
    fields_desc = [
        BitField("sn", 0, 16),
        BitField("e", 0, 8),
        BitField("r", 0, 8)
    ]

class SDAPULLayer(Packet):
    name = "SDAPULLayer"
    fields_desc = [
        BitField("dc", 0, 1),
        BitField("r", 0, 1),
        BitField("qfi", 0, 6)
    ]

class PDCPLayer(Packet):
    name = "PDCPLayer"
    fields_desc = [
        BitField("dc", 0, 4),
        BitField("r", 0, 4),
        BitField("sn", 0, 16)
    ]

class GTPULayer(Packet):
    name = "GTPULayer"
    fields_desc = [
        BitField("flags", 0, 8),
        BitField("type", 0, 8),
        BitField("length", 0, 16),
        BitField("teid", 0, 32),
        BitField("seq_num", 0, 16),
		BitField("qfi", 0, 8),
    ]

def calculate_payload_size_gtp(total_size):
    # Subtract the size of all headers from the total size
    header_size = 14 + 20 + 8 + 11 + 20 + 8 + 4 # Ethernet + IPv4 + UDP + GTP + ineer ip
    payload_size = total_size - header_size
    return payload_size

def calculate_payload_size_rlc(total_size):
    # Subtract the size of all headers from the total size
    header_size = 14 + 20 + 8 + 7 + 1 + 3 + 20 + 8  # Ethernet + IPv4 + UDP + SDAP + PDCP + RLC
    payload_size = total_size - header_size - 4
    return payload_size

def generate_gtp_packet(teid, seq_num,qfi, s, d, total_size):
	payload_size = calculate_payload_size_gtp(total_size)
	payload = Raw(b'x' * payload_size)
	eth_h = EthLayer(dmac=0x112233445566, smac=0x66778899aabb, type=0x0800)
	ipv4_h = IPv4Layer(version=4, ihl=5, diffserv=0, totalLen=total_size, identification=1234, flags=0,
                       fragOffset=0, ttl=64, protocol=17, hdrChecksum=0, srcAddr=s, dstAddr=d)
	udp_h = UDPLayer(srcPort=2152, dstPort=2152, len=8, checksum=0)
	gtpu_h = GTPULayer(flags=0x01, type=0xff, length=32, teid=teid, seq_num=seq_num,qfi=qfi)
	inner_ipv4_h = IPv4Layer(version=4, ihl=5, diffserv=0, totalLen=total_size, identification=1234, flags=0,
                       fragOffset=0, ttl=64, protocol=17, hdrChecksum=0, srcAddr=s, dstAddr=d)
	inner_udp_h = UDPLayer(srcPort=2153, dstPort=2152, len=8, checksum=0)

	gtp_packet = eth_h / ipv4_h / udp_h / gtpu_h / inner_ipv4_h / inner_udp_h / payload
	return gtp_packet

def generate_rlc_ack_ul_packet(s, d, total_size):
	payload_size = calculate_payload_size_rlc(total_size)
	payload = Raw(b'x' * payload_size)
	eth_h = EthLayer(dmac=0x112233445566, smac=0x66778899aabb, type=0x0800)
	ipv4_h = IPv4Layer(version=4, ihl=5, diffserv=0, totalLen=total_size, identification=1234, flags=0,
                       fragOffset=0, ttl=64, protocol=17, hdrChecksum=0, srcAddr=s, dstAddr=d)
	udp_h = UDPLayer(srcPort=8042, dstPort=2152, len=8, checksum=0)
	rlcack_h = RLCackmodeLayer(dc=1,p=0,si=0,r=0,snpadding=0,sn=0,teid=0)
	pdcp_h= PDCPLayer(dc=0,r=0,sn=0)
	sdapul_h= SDAPULLayer(dc=0,r=0,qfi=0)
	inner_ipv4_h = IPv4Layer(version=4, ihl=5, diffserv=0, totalLen=total_size, identification=1234, flags=0,
                       fragOffset=0, ttl=64, protocol=17, hdrChecksum=0, srcAddr=s, dstAddr=d)
	inner_udp_h = UDPLayer(srcPort=2153, dstPort=2152, len=8, checksum=0)

	gtp_packet = eth_h / ipv4_h / udp_h /  rlcack_h / pdcp_h / sdapul_h/ inner_ipv4_h / inner_udp_h / payload
	return gtp_packet

def generate_rlc_status_packet1(teid, sn, e_bit, s, d):
    eth_h = EthLayer(dmac=0x112233445566, smac=0x66778899aabb, type=0x0800)
    ip_h = IPv4Layer(version=4, ihl=5, diffserv=0, totalLen=60, identification=1234, flags=0,
                     fragOffset=0, ttl=64, protocol=17, hdrChecksum=0, srcAddr=s, dstAddr=d)
    udp_h = UDPLayer(srcPort=8052, dstPort=65359, len=8, checksum=0)

    rlc_status_h = RLCStatusLayer(dc=0, cpd=0, snpadding=0, sn=sn, e=e_bit, r=0, teid=teid)

    rlc_packet = eth_h / ip_h / udp_h / rlc_status_h
    return rlc_packet

def generate_rlc_status_packet(teid, sn, e_bit, s, d):
    eth_h = EthLayer(dmac=0x112233445566, smac=0x66778899aabb, type=0x0800)
    ip_h = IPv4Layer(version=4, ihl=5, diffserv=0, totalLen=60, identification=1234, flags=0,
                     fragOffset=0, ttl=64, protocol=17, hdrChecksum=0, srcAddr=s, dstAddr=d)
    udp_h = UDPLayer(srcPort=8052, dstPort=65359, len=8, checksum=0)

    rlc_status_h = RLCStatusLayer(dc=0, cpd=0, snpadding=0, sn=sn, e=e_bit, r=0, teid=teid)

    rlc_packet = eth_h / ip_h / udp_h / rlc_status_h
    return rlc_packet

def generate_rlc_nack_packet(teid, sn, e_bit, s, d):
    eth_h = EthLayer(dmac=0x112233445566, smac=0x66778899aabb, type=0x0800)
    ip_h = IPv4Layer(version=4, ihl=5, diffserv=0, totalLen=60, identification=1234, flags=0,
                     fragOffset=0, ttl=64, protocol=17, hdrChecksum=0, srcAddr=s, dstAddr=d)
    udp_h = UDPLayer(srcPort=8052, dstPort=65359, len=8, checksum=0)

    rlc_status_h = RLCStatusLayer(dc=0, cpd=0, snpadding=0, sn=sn, e=e_bit, r=0, teid=teid)

    rlc_nack_h = RLCNACKLayer(sn=sn, e=0, r=0)
    rlc_packet = eth_h / ip_h / udp_h / rlc_status_h / rlc_nack_h
    return rlc_packet


class ProxyStream:
	def __init__(self, command):
		self.pps = None
		self.percentage = None
		if command is None:
			self.enabled = False
			return
		args = command.split(',')
		if len(args) == 0:
			self.enabled = False
			return
		self.dct = {}
		for a in args:
			k, v = a.split('-')
			self.dct[k] = v
		if 'pps' in self.dct:
			self.pps = int(self.dct['pps'])
		if 'percentage' in self.dct:
			self.percentage = float(self.dct['percentage'])
		if 'imixp' in self.dct:
			self.imixp = float(self.dct['imixp'])

		if self.percentage is not None and self.pps is not None:
			self.enabled = False
		else:
			self.enabled = True

class STLS1(object):
	def __init__(self):
		self.mode = 0
		self.fsize = 64
		self.percentage = None
		self.pps = None
		self.uecount = 1

	def create_downlink_stream(self):
		maxval = 0x0a000000 + self.uecount
		vm = [
			
			STLVmTupleGen("tuple", ip_min=0, ip_max=16777215, port_min=0, port_max=4, limit_flows=4294967295, flags=1),
            #STLVmWrFlowVar (fv_name="tuple.port", pkt_offset= "GTPULayer.seq_num" ),
            STLVmWrFlowVar (fv_name="tuple.ip", pkt_offset= "GTPULayer.teid" ),

			STLVmFlowVar('dstip', min_value=0, max_value=16777215, size=4, step=257, op='random'),
		 	STLVmWrFlowVar(fv_name = 'dstip', pkt_offset = "IPv4Layer.dstAddr"), 
			 
			# STLVmFlowVar('dstPort', min_value=1, max_value=65535, size=2, step=1, op='inc'),
			# STLVmWrFlowVar(fv_name = 'dstPort', pkt_offset = "UDPLayer.dstPort"),
			# STLVmFlowVar('srcip', min_value=0, max_value=4294967295, size=4, step=1, op='inc'),
			# STLVmWrFlowVar(fv_name = 'srcip', pkt_offset = "IPv4Layer.srcAddr"), 	

            STLVmFlowVar('teid', min_value=0, max_value=63998, size=4, step=1, op='inc'),
			STLVmWrFlowVar(fv_name = 'teid', pkt_offset = "GTPULayer.teid"), 
			STLVmFlowVar('qfi', min_value=0, max_value=10, size=1, step=1, op='inc'),
			STLVmWrFlowVar(fv_name = 'qfi', pkt_offset = "GTPULayer.qfi"), 
		 	STLVmFixIpv4(offset = "IPv4Layer")
		 ]
		
		pkt= STLPktBuilder(pkt =generate_gtp_packet(0, 0,0, '10.0.0.0', '10.0.0.0',self.fsize), vm = vm)
		
		mode = None
		if self.downlink.percentage is not None:
			pass
			mode = STLTXCont(percentage = self.downlink.percentage)
			#mode = STLTXMultiBurst(pkts_per_burst = self.uplink.percentage)
			
		elif self.downlink.pps is not None:
			mode = STLTXCont(pps = self.downlink.pps)
			#mode = STLTXMultiBurst(pkts_per_burst = 2, pps=self.uplink.pps,ibg=0.0001)
			
		return STLStream(packet = pkt, mode = mode)

	def create_ack_stream(self):
		maxval = 0x0a000000 + self.uecount
		vm = [
			
			#STLVmTupleGen("tuple", ip_min=0, ip_max=1073741823, port_min=0, port_max=4, limit_flows=4294967295, flags=1),
            #STLVmWrFlowVar (fv_name="tuple.port", pkt_offset= "GTPULayer.seq_num" ),
            #STLVmWrFlowVar (fv_name="tuple.ip", pkt_offset= "GTPULayer.teid" ),

			STLVmFlowVar('dstip', min_value=0, max_value=4294967295, size=4, step=1, op='inc'),
		 	STLVmWrFlowVar(fv_name = 'dstip', pkt_offset = "IPv4Layer.dstAddr"), 
			 
			STLVmFlowVar('srcip', min_value=0, max_value=4294967295, size=4, step=1, op='inc'),
			STLVmWrFlowVar(fv_name = 'srcip', pkt_offset = "IPv4Layer.srcAddr"), 	

            STLVmFlowVar('teid', min_value=0, max_value=64000, size=4, step=1, op='inc'),
			STLVmWrFlowVar(fv_name = 'teid', pkt_offset = "RLCStatusLayer.teid"), 

			# STLVmFlowVar('sn', min_value=0, max_value=10000, size=4, step=1, op='inc'),
			# STLVmWrFlowVar(fv_name = 'sn', pkt_offset = "RLCStatusLayer.sn"), 
			
		 	STLVmFixIpv4(offset = "IPv4Layer")
		 ]
		
		pkt= STLPktBuilder(pkt =generate_rlc_status_packet(0, 0,0, '10.0.0.0', '10.0.0.0'), vm = vm)
		
		mode = None
		if self.ack.percentage is not None:
			pass
			mode = STLTXCont(percentage = self.ack.percentage)
			#mode = STLTXMultiBurst(pkts_per_burst = self.uplink.percentage)
			
		elif self.ack.pps is not None:
			mode = STLTXCont(pps = self.ack.pps)
			#mode = STLTXMultiBurst(pkts_per_burst = 2, pps=self.uplink.pps,ibg=0.0001)
			
		return STLStream(packet = pkt, mode = mode)


	def create_ack2_stream(self):
		maxval = 0x0a000000 + self.uecount
		vm = [
			
			#STLVmTupleGen("tuple", ip_min=0, ip_max=1073741823, port_min=0, port_max=4, limit_flows=4294967295, flags=1),
            #STLVmWrFlowVar (fv_name="tuple.port", pkt_offset= "GTPULayer.seq_num" ),
            #STLVmWrFlowVar (fv_name="tuple.ip", pkt_offset= "GTPULayer.teid" ),

			STLVmFlowVar('dstip', min_value=0, max_value=4294967295, size=4, step=1, op='inc'),
		 	STLVmWrFlowVar(fv_name = 'dstip', pkt_offset = "IPv4Layer.dstAddr"), 
			 
			STLVmFlowVar('srcip', min_value=0, max_value=4294967295, size=4, step=1, op='inc'),
			STLVmWrFlowVar(fv_name = 'srcip', pkt_offset = "IPv4Layer.srcAddr"), 	

            STLVmFlowVar('teid', min_value=0, max_value=10000, size=4, step=1, op='inc'),
			STLVmWrFlowVar(fv_name = 'teid', pkt_offset = "RLCStatusLayer.teid"), 

			STLVmFlowVar('sn', min_value=0, max_value=10000, size=4, step=1, op='inc'),
			STLVmWrFlowVar(fv_name = 'sn', pkt_offset = "RLCStatusLayer.sn"), 
			
		 	STLVmFixIpv4(offset = "IPv4Layer")
		 ]
		
		pkt= STLPktBuilder(pkt =generate_rlc_status_packet1(0, 0,0, '10.0.0.0', '10.0.0.0'), vm = vm)
		
		mode = None
		if self.ack2.percentage is not None:
			pass
			mode = STLTXCont(percentage = self.ack2.percentage)
			#mode = STLTXMultiBurst(pkts_per_burst = self.uplink.percentage)
			
		elif self.ack2.pps is not None:
			mode = STLTXCont(pps = self.ack2.pps)
			#mode = STLTXMultiBurst(pkts_per_burst = 2, pps=self.uplink.pps,ibg=0.0001)
			
		return STLStream(packet = pkt, mode = mode)

	def create_uplink_stream(self):
		maxval = 0x0a000000 + self.uecount
		vm = [
			
			STLVmFlowVar('srcip', min_value='10.0.0.0', max_value='10.0.255.0', size=4, step=1, op='inc'),
			STLVmWrFlowVar(fv_name = 'srcip', pkt_offset = "IPv4Layer.srcAddr"), 
			
            STLVmFlowVar('dstip', min_value='10.0.0.0', max_value='10.0.255.0', size=4, step=1, op='inc'),
			STLVmWrFlowVar(fv_name = 'dstip', pkt_offset = "IPv4Layer.dstAddr"), 
			
			STLVmFixIpv4(offset = "IPv4Layer")
		]
		#pkt = STLPktBuilder(pkt = base/pad, vm = vm)
		pkt= STLPktBuilder(pkt =generate_rlc_ack_ul_packet('10.0.0.0', '10.0.0.0',self.fsize), vm = vm)

		mode = None
		if self.uplink.percentage is not None:
			#pass
			mode = STLTXCont(percentage = self.uplink.percentage)
			#mode = STLTXMultiBurst(ppkts_per_burst = self.uplink.percentage)
		elif self.uplink.pps is not None:
			mode = STLTXCont(pps = self.uplink.pps)
			#mode = STLTXMultiBurst(pkts_per_burst = 2, pps=self.uplink.pps,ibg=0.0001)
		return STLStream(packet = pkt, mode = mode)




	def create_dllatency_stream(self):
		
		pkt = STLPktBuilder(pkt = generate_gtp_packet(1000,0,0, '10.0.0.1', '10.0.0.2',self.fsize))
		stats = STLFlowLatencyStats(pg_id = 13)
		return STLStream(packet = pkt, mode = STLTXCont(percentage = 1), flow_stats = stats)

	def create_ullatency_stream(self):
		
		pkt = STLPktBuilder(pkt = generate_rlc_ack_ul_packet('10.0.0.1', '10.0.0.150',self.fsize))
		stats = STLFlowLatencyStats(pg_id = 14)
		return STLStream(packet = pkt, mode = STLTXCont(percentage = 1), flow_stats = stats)
	
	def get_streams(self, tunables, **kwargs):
		parser = argparse.ArgumentParser(description='Argparser for {}'.format(os.path.basename(__file__)), formatter_class=argparse.ArgumentDefaultsHelpFormatter)
		parser.add_argument('--downlink', type=str,
			help = 'Downlink traffic pattern: type-amount, e.g. pps-10')
		parser.add_argument('--uplink', type=str,
			help = 'Uplink traffic pattern: type-amount, e.g. pps-10')
		parser.add_argument('--ack', type=str,
			help = 'ack traffic pattern: type-amount, e.g. pps-10')
		parser.add_argument('--ack2', type=str,
			help = 'ack traffic pattern: type-amount, e.g. pps-10')
		parser.add_argument('--pktsize', type=int, default=64, 
			help = 'Packet size to send, use -1 for imix')
		# parser.add_argument('--imixp', type=float, 
		# 	help = 'imix percent')
		parser.add_argument('--uecount', type=int, default=1,
			help = 'Number of UEs (unique src IPs)')
		parser.add_argument('--dllatency', action='store_true',
			help = 'dl Packet latency')
		parser.add_argument('--ullatency', action='store_true',
			help = 'ul Packet latency')
		
		args = parser.parse_args(tunables)
		self.fsize = args.pktsize
		self.uplink = ProxyStream(args.uplink)
		self.downlink = ProxyStream(args.downlink)
		self.ack = ProxyStream(args.ack)
		self.ack2 = ProxyStream(args.ack2)

		#self.imixp=args.imixp
		# if not self.uplink.enabled and not self.downlink.enabled:
		# 	print('Need either uplink or downlink traffic. ')
		# 	return None
		self.uecount = args.uecount
		streams = []
		if self.fsize != -1:
			if self.uplink.enabled:
				streams.append(self.create_uplink_stream())
			if self.downlink.enabled:
				streams.append(self.create_downlink_stream())	
			if self.ack.enabled:
				streams.append(self.create_ack_stream())	
			if self.ack2.enabled:
				streams.append(self.create_ack2_stream())	
		else: # imix
			
			self.fsize = 40
			if self.uplink.enabled:
				self.uplink.percentage = 0.58 * self.uplink.imixp
				streams.append(self.create_uplink_stream())
			if self.downlink.enabled:
				self.downlink.percentage = 0.58 * self.downlink.imixp
				streams.append(self.create_downlink_stream())	

			
			self.fsize = 576
			if self.uplink.enabled:
				self.uplink.percentage = 0.33 * self.uplink.imixp
				streams.append(self.create_uplink_stream())
			if self.downlink.enabled:
				self.downlink.percentage = 0.33 * self.downlink.imixp				
				streams.append(self.create_downlink_stream())	

			
			self.fsize = 1500
			if self.uplink.enabled:
				self.uplink.percentage = 0.08 * self.uplink.imixp
				streams.append(self.create_uplink_stream())
			if self.downlink.enabled:
				self.downlink.percentage = 0.08 * self.downlink.imixp
				streams.append(self.create_downlink_stream())	

		if args.dllatency:
			streams.append(self.create_dllatency_stream())
		if args.ullatency:
			streams.append(self.create_ullatency_stream())

		return streams


def register():
	return STLS1()

