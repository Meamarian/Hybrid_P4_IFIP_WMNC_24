/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2016 Intel Corporation
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <jansson.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <netinet/in.h>
#include <setjmp.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdbool.h>
#include <rte_common.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_memory.h>
#include <rte_memcpy.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_atomic.h>
#include <rte_cycles.h>
#include <rte_prefetch.h>
#include <rte_lcore.h>
#include <rte_per_lcore.h>
#include <rte_branch_prediction.h>
#include <rte_interrupts.h>
#include <rte_random.h>
#include <rte_debug.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_mempool.h>
#include <rte_mbuf.h>
#include <rte_string_fns.h>
#include <rte_hash.h>
#include <rte_jhash.h>
//#include <rte_ring_elem.h>
#include <rte_ring.h>
//#include <rte_ring_core.h>
//#include <rte_ring_core.h>

static volatile bool force_quit;

/* MAC updating enabled by default */
static int mac_updating = 1;

#define RTE_LOGTYPE_L2FWD RTE_LOGTYPE_USER1

#define MAX_PKT_BURST 32
#define BURST_TX_DRAIN_US 100 /* TX drain every ~100us */
#define MEMPOOL_CACHE_SIZE 512
#define MAX_PKT_BURST 32
#define UDP_PORT_GTPU 2152
#define UDP_PORT_HOST_GTP 2153
#define UDP_SPORT_RLC 8042
#define UDP_SPORT_RLCS 8052
#define UDP_SPORT_HRLC 8043
#define UDP_SPORT_HRLCS 8053
#define UDP_PORT_BUFFER 12345
#define ETHERTYPE_IPV4 0x0800
#define HASH_TABLE_SIZE 64000
#define HASH_TABLE_NAME "teid_qfi_table"
#define UL_HASH_TABLE_SIZE 64000
#define UL_HASH_TABLE_NAME "ul_teid_table"
#define RTE_PKTMBUF_HEADROOM 128
#define	HDR_MBUF_DATA_SIZE	(2 * RTE_PKTMBUF_HEADROOM)
#define CLONE_POOL_SIZE 100000U
struct rte_mbuf *clone_mbufs[CLONE_POOL_SIZE];
//unsigned int used_lcore_ids[MAX_LCORES];
//unsigned int num_used_lcores = 0;
unsigned int max_used_lcore = 0;
static const char *_MSG_POOL = "MSG_POOL0";
//static const char *_MSG_POOL = "MSG_POOL";
#define STR_TOKEN_SIZE 128
// static const char *_SEC_2_PRI = "SEC_2_PRI";
// static const char *_PRI_2_SEC1 = "PRI_2_SEC0";
// static const char *_PRI_2_SEC2 = "PRI_2_SEC1";
// static const char *_PRI_2_SEC3 = "PRI_2_SEC3";


struct rte_ring *send_ring0,*send_ring1,*send_ring2,*send_ring3, *send_ring4;
struct rte_ring *send_ring5,*send_ring6,*send_ring7;
struct rte_mempool *message_pool;
struct rte_mempool *message_pool1;
volatile int quit = 0;
const unsigned pool_size = 1000;
const unsigned pool_cache = 32*10;
const unsigned priv_data_sz = 0;
#define MBUF_SIZE (MAX_PKT_SIZE + sizeof(struct rte_mbuf) + RTE_PKTMBUF_HEADROOM)
#define MAX_PKT_SIZE 9600  // Adjust based on the maximum expected packet size


struct rte_mempool *my_clone_pool;;


//#define BURST_TX_DRAIN_US 1 /* TX drain every ~100us */ //100
//#define MEMPOOL_CACHE_SIZE 512

/*
 * Configurable number of RX/TX ring descriptors
 */
#define RTE_TEST_RX_DESC_DEFAULT 1024
#define RTE_TEST_TX_DESC_DEFAULT 1024
static uint16_t nb_rxd = RTE_TEST_RX_DESC_DEFAULT;
static uint16_t nb_txd = RTE_TEST_TX_DESC_DEFAULT;

/* ethernet addresses of ports */
static struct rte_ether_addr l2fwd_ports_eth_addr[RTE_MAX_ETHPORTS];

/* mask of enabled ports */
static uint32_t l2fwd_enabled_port_mask = 0;

/* list of enabled ports */
static uint32_t l2fwd_dst_ports[RTE_MAX_ETHPORTS];

struct port_pair_params {
#define NUM_PORTS	2
	uint16_t port[NUM_PORTS];
} __rte_cache_aligned;

static struct port_pair_params port_pair_params_array[RTE_MAX_ETHPORTS / 2];
static struct port_pair_params *port_pair_params;
static uint16_t nb_port_pair_params;

static unsigned int l2fwd_rx_queue_per_lcore = 1;

#define MAX_RX_QUEUE_PER_LCORE 16
#define MAX_TX_QUEUE_PER_PORT 16
struct lcore_queue_conf {
	unsigned n_rx_port;
	unsigned rx_port_list[MAX_RX_QUEUE_PER_LCORE];
} __rte_cache_aligned;
struct lcore_queue_conf lcore_queue_conf[RTE_MAX_LCORE];

static struct rte_eth_dev_tx_buffer *tx_buffer[RTE_MAX_ETHPORTS];

// static struct rte_eth_conf port_conf = {
// 	.rxmode = {
// 		.split_hdr_size = 0,
// 	},
// 	.txmode = {
// 		.mq_mode = ETH_MQ_TX_NONE,
// 	},
// };

static struct rte_eth_conf port_conf = {
	.rxmode = {
        //.mq_mode = ETH_MQ_RX_NONE,
        .max_rx_pkt_len = 1518,//RTE_ETHER_MAX_LEN,
		.split_hdr_size = 0,
        //.offloads = BAAS_RX_OFFLOADS,
	},
	.txmode = {
		.mq_mode = ETH_MQ_TX_NONE,
	},
};

struct rte_mempool * l2fwd_pktmbuf_pool = NULL;

/* Per-port statistics struct */
struct l2fwd_port_statistics {
	uint64_t tx;
	uint64_t rx;
	uint64_t cloned;
	uint64_t dropped;
} __rte_cache_aligned;
struct l2fwd_port_statistics port_statistics[RTE_MAX_ETHPORTS];
struct l2fwd_port_statistics ring_statistics[8];

#define MAX_TIMER_PERIOD 86400 /* 1 day max */
/* A tsc-based timer responsible for triggering statistics printout */
static uint64_t timer_period = 10; /* default period is 10 seconds */

struct udp_hdr {
	uint16_t src_port;    /**< UDP source port. */
	uint16_t dst_port;    /**< UDP destination port. */
	uint16_t dgram_len;   /**< UDP datagram length */
	uint16_t dgram_cksum; /**< UDP datagram checksum */
} __attribute__((__packed__));

struct ipv4_hdr {
	uint8_t  version_ihl;		/**< version and header length */
	uint8_t  type_of_service;	/**< type of service */
	uint16_t total_length;	/**< length of packet */
	uint16_t packet_id;		/**< packet ID */
	uint16_t fragment_offset;	/**< fragmentation offset */
	uint8_t  time_to_live;		/**< time to live */
	uint8_t  next_proto_id;		/**< protocol ID */
	uint32_t hdr_checksum;	/**< header checksum */
	uint32_t src_addr;		/**< source address */
	uint32_t dst_addr;		/**< destination address */
} __attribute__((__packed__));

/* Define the hash table structures */
struct gtp_header {
    uint8_t flags;
    uint8_t type;
    uint16_t length;
    uint32_t teid;
    uint16_t seq_num;
    uint8_t qfi;
} __attribute__((__packed__));

struct rlc_ack_mode_header {
    uint8_t dc_p_si_r; // dc:1, p:1, si:2, r:2
    uint16_t sn;
    uint32_t teid;
    //uint8_t rbnum;
} __attribute__((__packed__));

struct pdcp_header {
    uint8_t dc_r;
    //uint8_t r;
    uint16_t sn;
} __attribute__((__packed__));

struct sdap_header {
    uint8_t rdi_rqi_qfi;
} __attribute__((__packed__));

// struct payload_header {
//     uint64_t rdi_rqi_qfi;
// 	uint64_t rdi_rqi_qfi;
// 	uint64_t rdi_rqi_qfi;
// } __attribute__((__packed__));

struct metadata {
    uint32_t teid;
    uint8_t *rbnumber;
    uint8_t *rqibool;
};

struct teid_qfi_entry {
    uint32_t teid;
    uint8_t rbnumber;
    uint8_t rqibool;
};

struct ul_teid_entry {
    uint32_t teid;
};

#define HASH_TABLE_SIZE 64000
#define HASH_TABLE_NAME "teid_qfi_table"
#define UL_HASH_TABLE_SIZE 64000
#define UL_HASH_TABLE_NAME "ul_teid_table"

struct rte_hash *teid_qfi_table;
struct rte_hash *ul_teid_table;

/* Function declarations */
// void set_sdap_dl(struct rte_mbuf *m, uint8_t rqi);
// void set_pdcp(struct rte_mbuf *m);
// void set_rlc_ack_mode(struct rte_mbuf *m, uint16_t sn);
// void swap_mac_addresses(struct rte_ether_hdr *eth_hdr);
// void set_egress_port(struct rte_mbuf *m, uint16_t port);
// void parse_gtp(struct rte_mbuf *m, struct metadata *meta);
// void parse_rlc(struct rte_mbuf *m, struct metadata *meta);
// void parse_pdcp(struct rte_mbuf *m, struct metadata *meta);
// void parse_sdap(struct rte_mbuf *m, struct metadata *meta);
// void handle_packet(struct rte_mbuf *m,struct rte_ring *send_ringx,unsigned portid, int clone_idx);
// //void handle_packet(struct rte_mbuf *m,struct rte_ring *send_ringx,unsigned portid);
// void populate_hash_table(const char *filename);
// void populate_ul_hash_table(const char *filename);



void gtp_decapsulate(struct rte_mbuf *m,uint32_t teid) {

	struct rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(m, struct ether_hdr *);
    struct rte_ipv4_hdr *ipv4_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 14);
    // Calculate the total header length to be removed
    uint16_t gtp_header_len = sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + sizeof(struct gtp_header);

    // Adjust the packet buffer to remove the GTP header and access the payload
    //if (rte_pktmbuf_trim(m, gtp_header_len) < 0) {
	if (rte_pktmbuf_adj(m, gtp_header_len) < 0) {
        printf("Failed to decapsulate GTP header\n");
    } else {
        //printf("GTP header decapsulated\n");
    }

    // struct sdap_header *sdap_hdr;

    // // Append space for SDAP header
    // sdap_hdr = (struct sdap_header *)rte_pktmbuf_prepend(m, sizeof(struct sdap_header));
    // if (sdap_hdr == NULL) {
    //     printf("Failed to append SDAP header\n");
    //     return;
    // }


    // // Set the SDAP header values (using a default RQI value)
    // sdap_hdr->rdi_rqi_qfi = 0; // Example default RQI value

    // struct pdcp_header *pdcp_hdr;

    // // Append space for PDCP header
    // pdcp_hdr = (struct pdcp_header *)rte_pktmbuf_prepend(m, sizeof(struct pdcp_header));
    // if (pdcp_hdr == NULL) {
    //     printf("Failed to append PDCP header\n");
    //     return;
    // }

    // // Set the PDCP header values with default values
    // pdcp_hdr->dc_r = 0; // Default value
    // pdcp_hdr->sn = rte_cpu_to_be_16(0); // Default SN value
    // //printf("PDCP header appended with default SN = %u\n", pdcp_hdr->sn);

    // struct rlc_ack_mode_header *rlc_hdr;

    // // Append space for RLC ACK mode header
    // rlc_hdr = (struct rlc_ack_mode_header *)rte_pktmbuf_prepend(m, sizeof(struct rlc_ack_mode_header));
    // if (rlc_hdr == NULL) {
    //     printf("Failed to append RLC ACK mode header\n");
    //     return;
    // }

    // // Set the RLC ACK mode header values with default values
    // rlc_hdr->dc_p_si_r = 0; // Default value
    // rlc_hdr->sn = 0; // Default SN value
    // rlc_hdr->teid = teid; // Default TEID value



    // // Create and prepend the new UDP header
    // struct udp_hdr *udp_hdr = (struct udp_hdr *)rte_pktmbuf_prepend(m, sizeof(struct udp_hdr));
    // if (udp_hdr == NULL) {
    //     printf("Failed to prepend UDP header\n");
    //     return;
    // }
    // udp_hdr->src_port = rte_cpu_to_be_16(8043); // Example source port
    // udp_hdr->dst_port = 2134; // GTP-U port (2152)
    // udp_hdr->dgram_len = 0;

//    // Create and prepend the new outer IP header
//     struct rte_ipv4_hdr *ip_hdr = (struct rte_ipv4_hdr *)rte_pktmbuf_prepend(m, sizeof(struct rte_ipv4_hdr));
//     if (ip_hdr == NULL) {
//         printf("Failed to prepend IP header\n");
//         return;
//     }
//     ip_hdr->version_ihl = (4 << 4) | (sizeof(struct rte_ipv4_hdr) / 4);
//     // ip_hdr->type_of_service = 0;
//     // ip_hdr->total_length = rte_cpu_to_be_16(rte_pktmbuf_pkt_len(m));
//     // ip_hdr->packet_id = rte_cpu_to_be_16(1);
//     // ip_hdr->fragment_offset = 0;
//     // ip_hdr->time_to_live = 64;
//     ip_hdr->next_proto_id = IPPROTO_UDP; // Indicates that UDP follows
//     // ip_hdr->hdr_checksum = 0; // Will be calculated by hardware if enabled
//     // ip_hdr->src_addr = rte_cpu_to_be_32(0xc0a80101); // Example source IP (192.168.1.1)
//     // ip_hdr->dst_addr = rte_cpu_to_be_32(0xc0a80102); // Example destination IP (192.168.1.2)

//     // // Calculate IP checksum
//     // ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

		// Prepend the original Ethernet header to the packet
	// struct rte_ipv4_hdr *new_ipv4_hdr = (struct rte_ipv4_hdr *)rte_pktmbuf_prepend(m, sizeof(struct rte_ipv4_hdr));
	// if (new_ipv4_hdr == NULL) {
	// 	printf("Failed to prepend Ethernet header\n");
	// 	return;
	// }

	// // Copy the original Ethernet header values
	// rte_memcpy(new_ipv4_hdr, ipv4_hdr, sizeof(struct rte_ipv4_hdr));


		// Prepend the original Ethernet header to the packet
	struct ether_hdr *new_eth_hdr = (struct ether_hdr *)rte_pktmbuf_prepend(m, sizeof(struct rte_ether_hdr));
	if (new_eth_hdr == NULL) {
		printf("Failed to prepend Ethernet header\n");
		return;
	}

	// Copy the original Ethernet header values
	rte_memcpy(new_eth_hdr, eth_hdr, sizeof(struct rte_ether_hdr));



}

void gtp_encapsulate(struct rte_mbuf *m,uint32_t teid) {

	struct rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(m, struct ether_hdr *);
	//struct ipv4_hdr *ipv4_hdr = (struct ipv4_hdr *)(eth_hdr + 14);

    uint16_t other_header_len = sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + sizeof(struct rlc_ack_mode_header)+ sizeof(struct pdcp_header)+ sizeof(struct sdap_header);

	if (rte_pktmbuf_adj(m, other_header_len) < 0) {
        printf("Failed to remove other header\n");

	}
	// Create and prepend the new GTP-U header
    struct gtp_header *gtp_hdr = (struct gtp_hdr *)rte_pktmbuf_prepend(m, sizeof(struct gtp_header));
    if (gtp_hdr == NULL) {
        printf("Failed to prepend GTP header\n");
        return;
    }
    gtp_hdr->teid = rte_cpu_to_be_32(teid); 
    //gtp_hdr->length = rte_cpu_to_be_16();

    // Create and prepend the new UDP header
    struct udp_hdr *udp_hdr = (struct udp_hdr *)rte_pktmbuf_prepend(m, sizeof(struct udp_hdr));
    if (udp_hdr == NULL) {
        printf("Failed to prepend UDP header\n");
        return;
    }
    udp_hdr->src_port = rte_cpu_to_be_16(2153); // Example source port
    udp_hdr->dst_port = 2134; // GTP-U port (2152)
    udp_hdr->dgram_len = 0;

    //Create and prepend the new outer IP header
    struct ipv4_hdr *ip_hdr = (struct ipv4_hdr *)rte_pktmbuf_prepend(m, sizeof(struct ipv4_hdr));
    if (ip_hdr == NULL) {
        printf("Failed to prepend IP header\n");
        return;
    }
    ip_hdr->version_ihl = (4 << 4) | (sizeof(struct ipv4_hdr) / 4);
    ip_hdr->type_of_service = 0;
    ip_hdr->total_length = rte_cpu_to_be_16(rte_pktmbuf_pkt_len(m));
    ip_hdr->packet_id = rte_cpu_to_be_16(1);
    ip_hdr->fragment_offset = 0;
    ip_hdr->time_to_live = 64;
    ip_hdr->next_proto_id = IPPROTO_UDP; // Indicates that UDP follows
    ip_hdr->hdr_checksum = 0; // Will be calculated by hardware if enabled
    // ip_hdr->src_addr = rte_cpu_to_be_32(0xc0a80101); // Example source IP (192.168.1.1)
    // ip_hdr->dst_addr = rte_cpu_to_be_32(0xc0a80102); // Example destination IP (192.168.1.2)

    // Calculate IP checksum
    ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

		// Prepend the original Ethernet header to the packet
	// struct ipv4_hdr *new_ipv4_hdr = (struct ipv4_hdr *)rte_pktmbuf_prepend(m, sizeof(struct ipv4_hdr));
	// if (new_ipv4_hdr == NULL) {
	// 	printf("Failed to prepend Ethernet header\n");
	// 	return;
	// }

	// // Copy the original Ethernet header values
	// //rte_memcpy(new_ipv4_hdr, ipv4_hdr, sizeof(struct ipv4_hdr));
	
	struct ether_hdr *new_eth_hdr = (struct ether_hdr *)rte_pktmbuf_prepend(m, sizeof(struct rte_ether_hdr));
	if (new_eth_hdr == NULL) {
		printf("Failed to prepend Ethernet header\n");
		return;
	}

	// Copy the original Ethernet header values
	rte_memcpy(new_eth_hdr, eth_hdr, sizeof(struct rte_ether_hdr));
}


void swap_mac_addresses(struct rte_ether_hdr *eth_hdr) {
    struct rte_ether_addr temp;
    rte_ether_addr_copy(&eth_hdr->s_addr, &temp);
    rte_ether_addr_copy(&eth_hdr->d_addr, &eth_hdr->s_addr);
    rte_ether_addr_copy(&temp, &eth_hdr->d_addr);
}


void set_egress_port(struct rte_mbuf *m, uint16_t port) {
    m->port = port;
}

void parse_gtp(struct rte_mbuf *m, struct metadata *meta) {
    struct gtp_header *gtp_hdr = rte_pktmbuf_mtod_offset(m, struct gtp_header *, sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr));
    meta->teid = rte_be_to_cpu_32(gtp_hdr->teid);
}

void parse_rlc(struct rte_mbuf *m, struct metadata *meta) {
    struct rlc_ack_mode_header *rlc_hdr = rte_pktmbuf_mtod_offset(m, struct rlc_ack_mode_header *, sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + sizeof(struct gtp_header));
    //meta->rbnumber = rlc_hdr->rbnum;
}

void parse_pdcp(struct rte_mbuf *m, struct metadata *meta) {
    struct pdcp_header *pdcp_hdr = rte_pktmbuf_mtod_offset(m, struct pdcp_header *, sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + sizeof(struct gtp_header) + sizeof(struct rlc_ack_mode_header));
    // No specific metadata to store from PDCP header for now
}

void parse_sdap(struct rte_mbuf *m, struct metadata *meta) {
    struct sdap_header *sdap_hdr = rte_pktmbuf_mtod_offset(m, struct sdap_header *, sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + sizeof(struct gtp_header) + sizeof(struct rlc_ack_mode_header) + sizeof(struct pdcp_header));
    //meta->rqibool = sdap_hdr->rdi_rqi_qfi & 0x01;
}

uint32_t calculate_fnv_hash(uint32_t src_ip, uint32_t dst_ip) {
    uint64_t addr_pair = ((uint64_t)src_ip << 32) | dst_ip;
    uint32_t hash = 2166136261;
    hash = (hash ^ addr_pair) * 16777219;
    return hash;
}

// Function to lookup a TEID in teid_qfi_table
void lookup_teid_in_teid_qfi_table(uint32_t teid) {
    uint8_t *rbnumber; // Define a pointer to hold the returned rbnumber
	//struct metadata x;
    int ret = rte_hash_lookup_data(teid_qfi_table, &teid, (void **)&rbnumber);

    if (ret < 0) {
        printf("Failed to lookup TEID = %u in teid_qfi_table\n", teid);
    } else {
        //printf("Lookup TEID = %u: RB = %u\n", teid, rbnumber);
    }
}

// Function to lookup an IP address in ul_teid_table
void lookup_ip_in_ul_teid_table(const char *ip_str) {
    uint32_t ip;
	uint32_t *teid;
    if (inet_pton(AF_INET, ip_str, &ip) != 1) { // Convert the IP string to a uint32_t format
        printf("Invalid IP address format: %s\n", ip_str);
        return;
    }
    //ip = rte_cpu_to_be_32(ip); // Ensure the IP is in the correct endian format for lookup

    //struct ul_teid_entry *entry; // Pointer to hold the ul_teid_entry returned by the lookup
    int ret = rte_hash_lookup_data(ul_teid_table, &ip, (void **)&teid);

    if (ret < 0) {
        printf("Failed to lookup IP = %s in ul_teid_table\n", ip_str);
    } else {
        //printf("Lookup IP = %s: TEID = %u\n", ip_str, teid);
    }
	
}

// Function to print mbuf details
void print_mbuf(struct rte_mbuf *m) {
    // Print mbuf metadata
    printf("mbuf details:\n");
    printf("Packet length: %u\n", rte_pktmbuf_pkt_len(m));
    printf("Data length: %u\n", rte_pktmbuf_data_len(m));
    printf("Buffer length: %u\n", m->buf_len);
    printf("Refcount: %u\n", rte_mbuf_refcnt_read(m));

    // Print packet data as hex
    printf("Packet data (hex):\n");
    uint8_t *data = rte_pktmbuf_mtod(m, uint8_t *);
    for (uint32_t i = 0; i < rte_pktmbuf_data_len(m); i++) {
        printf("%02x ", data[i]);
        if ((i + 1) % 16 == 0) {
            printf("\n");
        }
    }
    printf("\n");

    // Optional: Interpret the packet data as network headers (Ethernet, IP, UDP)
    struct rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(m, struct rte_ether_hdr *);
    printf("Ethernet Header:\n");
    printf("  Src MAC: %02X:%02X:%02X:%02X:%02X:%02X\n",
           eth_hdr->s_addr.addr_bytes[0], eth_hdr->s_addr.addr_bytes[1],
           eth_hdr->s_addr.addr_bytes[2], eth_hdr->s_addr.addr_bytes[3],
           eth_hdr->s_addr.addr_bytes[4], eth_hdr->s_addr.addr_bytes[5]);
    printf("  Dst MAC: %02X:%02X:%02X:%02X:%02X:%02X\n",
           eth_hdr->d_addr.addr_bytes[0], eth_hdr->d_addr.addr_bytes[1],
           eth_hdr->d_addr.addr_bytes[2], eth_hdr->d_addr.addr_bytes[3],
           eth_hdr->d_addr.addr_bytes[4], eth_hdr->d_addr.addr_bytes[5]);
    printf("  EtherType: 0x%04X\n", rte_be_to_cpu_16(eth_hdr->ether_type));

    // // Check if packet is IPv4
    // if (rte_be_to_cpu_16(eth_hdr->ether_type) == RTE_ETHER_TYPE_IPV4) {
    //     struct rte_ipv4_hdr *ipv4_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 14);
    //     //printf("IPv4 Header:\n");
    //     // printf("  Src IP: %s\n", inet_ntoa(*(struct in_addr *)&ipv4_hdr->src_addr));
    //     // printf("  Dst IP: %s\n", inet_ntoa(*(struct in_addr *)&ipv4_hdr->dst_addr));

	// 	struct rte_udp_hdr *udp_hdr = (struct rte_ipv4_hdr *)(ipv4_hdr + 20);;
	// 	printf("UDP Header:\n");
	// 	printf("  src Port: %u\n", rte_be_to_cpu_16(udp_hdr->src_port));
	// 	printf("  Dst Port: %u\n", rte_be_to_cpu_16(udp_hdr->dst_port));
       
    // }
	
}




void handle_packet(struct rte_mbuf *m,struct rte_ring *send_ringx,unsigned portid,int clone_idx) {
//void handle_packet(struct rte_mbuf *m,struct rte_ring *send_ringx,unsigned portid) {

	//printf("enter handle pkt\n");
    struct rte_ether_hdr *eth_hdr;
    struct rte_ipv4_hdr *ipv4_hdr;
    struct rte_udp_hdr *udp_hdr;
	struct gtp_header *gtp_hdr;
    struct metadata meta;
    struct teid_qfi_entry *entry;
    struct ul_teid_entry *ul_entry;
	uint8_t *rbnumber;
	uint32_t *lookedteid;

	uint8_t *pkt_data = rte_pktmbuf_mtod(m, uint8_t *);
	uint16_t src_udp_port = rte_be_to_cpu_16(*(uint16_t *)(pkt_data + 14 + 20 ));

	//printf("  src Port: %u\n", src_udp_port);
    if (src_udp_port == UDP_PORT_HOST_GTP) {
				//printf("GTP pkt arrived\n");
				meta.teid  = rte_be_to_cpu_32(*(uint32_t *)(pkt_data + 14 + 20 + 8 + 4));
				// uint16_t sn=  rte_be_to_cpu_16(*(uint16_t *)(pkt_data + 14 + 20 + 8 + 8));
				// uint64_t combined_value = ((uint64_t)meta.teid << 16) | (sn & 0xFFFF);

				// printf(" teid: %u\n", meta.teid);
				// printf(" sn: %u\n", sn);
				// printf(" c: %u\n", combined_value);

                // struct rte_mbuf *cloned_pkt = rte_pktmbuf_clone(m, my_clone_pool);
                // if (cloned_pkt != NULL) {
                //     // uint32_t src_ip = rte_be_to_cpu_32(ipv4_hdr->s_addr);
                //     //  uint32_t dst_ip = rte_be_to_cpu_32(ipv4_hdr->d_addr);
                //     //  uint32_t crc_hash_value = calculate_fnv_hash(src_ip, dst_ip);

                //      //   if (crc_hash_value % 2 == 0) {
				// 		printf("send to ring %s\n", send_ringx);
                //     	rte_ring_enqueue(send_ringx, (void *) cloned_pkt);
                //      //} else {
                //         // rte_ring_enqueue(send_ring2, (void *) cloned_pkt);
                //     }
				//static int clone_idx = 0;

				// Fetch a pre-allocated mbuf from the clone pool
				struct rte_mbuf *deep_copy_pkt = clone_mbufs[clone_idx];
				//clone_idx = (clone_idx + 1) % CLONE_POOL_SIZE;

				// Allocate a new mbuf from the pool for the deep copy
				// struct rte_mbuf *deep_copy_pkt = rte_pktmbuf_alloc(my_clone_pool);
				// struct rte_mbuf *deep_copy_pkt ;//= rte_pktmbuf_alloc(l2fwd_pktmbuf_pool);

				// if (deep_copy_pkt == NULL) {
				// 	printf("Failed to allocate new mbuf for deep copy\n");
				// 	return;
				// }

				// // Set the length of the new mbuf to match the original
				// deep_copy_pkt->pkt_len = m->pkt_len;
				// deep_copy_pkt->data_len = m->data_len;

				// // Copy the packet data from the original mbuf to the new mbuf
				// rte_memcpy(rte_pktmbuf_mtod(deep_copy_pkt, void *), rte_pktmbuf_mtod(m, void *), m->pkt_len);

				// // Ensure the deep copy has the correct mbuf headroom
				// deep_copy_pkt->data_off = m->data_off;

				// // Verify the deep copy
				// if (deep_copy_pkt->pkt_len != m->pkt_len || deep_copy_pkt->data_len != m->data_len) {
				// 	printf("Mismatch in packet length after copying\n");
				// 	rte_pktmbuf_free(deep_copy_pkt);
				// 	port_statistics[portid].dropped += 1;
				// 	return;
				// }
				// else{
				// 	port_statistics[portid].cloned += 1;
				// }

				// // rte_pktmbuf_free(deep_copy_pkt);
				
				// //Enqueue the deep copied packet to the ring
				// if (rte_ring_enqueue(send_ringx, (void *)deep_copy_pkt) < 0) {
				// 	// Handle enqueue failure
				// 	printf("Failed to enqueue the packet\n");
				// 	rte_pktmbuf_free(deep_copy_pkt);
				// } else {
				// 	ring_statistics[rte_lcore_id()-2].tx += 1;
				// 	//l2fwd_simple_forward(deep_copy_pkt,portid);
				// 	//printf("Packet successfully enqueued to ring %s\n", send_ringx);
				// }

				int ret = rte_hash_lookup_data(teid_qfi_table, &meta.teid, (void **)&rbnumber);
				//int ret=1;
                if (ret >= 0) {
					//printf("TEID %u is founded in hash table\n", meta.teid);
                    if (rbnumber == 0) {
						//gtp_decapsulate(m,meta.teid);
						//append_dummy_data(m, 26);
                    } else if (rbnumber == 1) {
						//gtp_decapsulate(m,meta.teid);
						//append_dummy_data(m, 26);

                    } else {
						printf("gtp rbnumber %u is invalid\n", rbnumber);
						port_statistics[portid].dropped += 1;
						rte_pktmbuf_free(m);
					}
                } else {
                    printf("TEID %u not found in hash table\n", meta.teid);
					port_statistics[portid].dropped += 1;
					rte_pktmbuf_free(m);
                }


            } else if (src_udp_port == UDP_SPORT_RLCS) {
                // Handle RLC status
            } else if (src_udp_port == UDP_SPORT_RLC) {
				meta.rbnumber=0;
                if (meta.rbnumber == 0) {
                    int ret = rte_hash_lookup_data(ul_teid_table, &ipv4_hdr->dst_addr, (void **)&lookedteid);
                    if (ret >= 0) {
						gtp_encapsulate(m,lookedteid);
                    } else {
                        printf("IP %u not found in UL hash table\n", ipv4_hdr->dst_addr);
						port_statistics[portid].dropped += 1;
						rte_pktmbuf_free(m);
                    }
                } else if (meta.rbnumber == 1) {
                    int ret = rte_hash_lookup_data(ul_teid_table, &ipv4_hdr->dst_addr, (void **)&lookedteid);
                    if (ret >= 0) {
						gtp_encapsulate(m,lookedteid);
                    } else {
                        printf("IP %u not found in UL hash table\n", ipv4_hdr->dst_addr);
						port_statistics[portid].dropped += 1;
						rte_pktmbuf_free(m);
                    }
                } else {
					printf("rlc rbnumber %u is invalid\n", meta.rbnumber);
					port_statistics[portid].dropped += 1;
					rte_pktmbuf_free(m);
				}
            }


    //}
}




void populate_teid_qfi_table(const char *filename) {
    json_t *root;
    json_error_t error;

    root = json_load_file(filename, 0, &error);
    if (!root) {
        fprintf(stderr, "Error: on line %d: %s\n", error.line, error.text);
        return;
    }

    json_t *tables = json_object_get(root, "tables");
    if (!json_is_object(tables)) {
        fprintf(stderr, "Error: 'tables' is not an object\n");
        json_decref(root);
        return;
    }

    json_t *teid_qfi_table_json = json_object_get(tables, "ingress::TEIDs_QFIs_to_RBs_RQI");
    if (!json_is_object(teid_qfi_table_json)) {
        fprintf(stderr, "Error: 'ingress::TEIDs_QFIs_to_RBs_RQI' is not an object\n");
        json_decref(root);
        return;
    }

    json_t *rules_array = json_object_get(teid_qfi_table_json, "rules");
    if (!json_is_array(rules_array)) {
        fprintf(stderr, "Error: 'rules' is not an array\n");
        json_decref(root);
        return;
    }

    struct rte_hash_parameters hash_params = {
        .name = HASH_TABLE_NAME,
        .entries = HASH_TABLE_SIZE,
        .key_len = sizeof(uint32_t),
        .hash_func = rte_jhash,
        .hash_func_init_val = 0,
    };

    teid_qfi_table = rte_hash_create(&hash_params);
    if (!teid_qfi_table) {
        perror("Failed to create hash table");
        json_decref(root);
        return;
    }
	int count = 0;
    size_t index;
    json_t *value;
    json_array_foreach(rules_array, index, value) {
        json_t *match = json_object_get(value, "match");
        json_t *action = json_object_get(value, "action");

        json_t *teid_val = json_object_get(json_object_get(match, "scalars.qos_metadata_t@teid_qfi"), "value");
        json_t *rb_val = json_object_get(json_object_get(action, "data"), "rb");
        json_t *rqi_val = json_object_get(json_object_get(action, "data"), "rqi");

        if (!json_is_string(teid_val) || !json_is_object(rb_val) || !json_is_object(rqi_val)) {
            fprintf(stderr, "Error: Missing expected TEID, RB, or RQI value in JSON\n");
            continue;  // Skip this entry
        }

        const char *teid_str = json_string_value(teid_val);
        const char *rb_str = json_string_value(json_object_get(rb_val, "value"));
        const char *rqi_str = json_string_value(json_object_get(rqi_val, "value"));

        if (!teid_str || !rb_str || !rqi_str) {
            fprintf(stderr, "Error: Null value for TEID, RB, or RQI in JSON\n");
            continue;  // Skip this entry
        }

        uint32_t teid = (uint32_t)strtol(teid_str, NULL, 10);
        uint16_t rb = (uint16_t)strtol(rb_str, NULL, 10);
        uint8_t rqi = (uint8_t)strtol(rqi_str, NULL, 10);

        struct teid_qfi_entry entry;
        entry.teid = teid;
        entry.rbnumber = rb;
        entry.rqibool = rqi;

		
        int ret = rte_hash_add_key_data(teid_qfi_table, &entry.teid, (void * )rb);
        if (ret < 0) {
            printf("Failed to add entry to teid_qfi_table for TEID %u\n", entry.teid);
        } else if (count < 5) {
            // Print first 5 entries added
            printf("Added to teid_qfi_table: TEID = %u, RB = %u \n", entry.teid, rb);
            count++;
        }
    }

    json_decref(root);
}

uint32_t rte_ipv4_str_to_addr(const char *ip_str) {
    struct in_addr ip_addr;
    if (inet_aton(ip_str, &ip_addr)) {
        return rte_be_to_cpu_32(ip_addr.s_addr);
    }
    return 0;
}

void populate_ul_teid_table(const char *filename) {
    json_t *root;
    json_error_t error;

    root = json_load_file(filename, 0, &error);
    if (!root) {
        fprintf(stderr, "Error: on line %d: %s\n", error.line, error.text);
        return;
    }

    json_t *tables = json_object_get(root, "tables");
    if (!json_is_object(tables)) {
        fprintf(stderr, "Error: 'tables' is not an object\n");
        json_decref(root);
        return;
    }

    json_t *ul_teid_table_json = json_object_get(tables, "ingress::UL_ASSIGN_TEID");
    if (!json_is_object(ul_teid_table_json)) {
        fprintf(stderr, "Error: 'ingress::UL_ASSIGN_TEID' is not an object\n");
        json_decref(root);
        return;
    }

    json_t *rules_array = json_object_get(ul_teid_table_json, "rules");
    if (!json_is_array(rules_array)) {
        fprintf(stderr, "Error: 'rules' is not an array\n");
        json_decref(root);
        return;
    }

    struct rte_hash_parameters hash_params = {
        .name = UL_HASH_TABLE_NAME,
        .entries = UL_HASH_TABLE_SIZE,
        .key_len = sizeof(uint32_t),
        .hash_func = rte_jhash,
        .hash_func_init_val = 0,
    };

    ul_teid_table = rte_hash_create(&hash_params);
    if (!ul_teid_table) {
        perror("Failed to create UL hash table");
        json_decref(root);
        return;
    }
	int count = 0;

    size_t index;
    json_t *value;
    json_array_foreach(rules_array, index, value) {
        json_t *match = json_object_get(value, "match");
        json_t *action = json_object_get(value, "action");

        json_t *ip_val = json_object_get(json_object_get(match, "ipv4.dstAddr"), "value");
        json_t *teid_val = json_object_get(json_object_get(action, "data"), "teid");

        if (!json_is_string(ip_val) || !json_is_object(teid_val)) {
            fprintf(stderr, "Error: Missing expected IP or TEID value in JSON\n");
            continue;  // Skip this entry
        }

        const char *ip_str = json_string_value(ip_val);
        const char *teid_str = json_string_value(json_object_get(teid_val, "value"));

        if (!ip_str || !teid_str) {
            fprintf(stderr, "Error: Null value for IP or TEID in JSON\n");
            continue;  // Skip this entry
        }

        uint32_t ip;
        inet_pton(AF_INET, ip_str, &ip);
        uint32_t teid = (uint32_t)strtol(teid_str, NULL, 10);

        struct ul_teid_entry entry = { teid };
		int ret = rte_hash_add_key_data(ul_teid_table, &ip,  (void * ) teid);
        if (ret < 0) {
            printf("Failed to add entry to ul_teid_table for IP %s\n", ip_str);
        } else if (count < 5) {
            // Print first 5 entries added
            printf("Added to ul_teid_table: IP = %s, TEID = %u\n", ip_str, teid);
            count++;
        }
    }

    json_decref(root);
}



/* Print out statistics on packets dropped */
static void
print_stats(void)
{
	uint64_t total_packets_dropped, total_packets_tx, total_packets_rx,total_packets_cloned,x0,x1;
	unsigned portid;

	total_packets_dropped = 0;
	total_packets_tx = 0;
	total_packets_rx = 0;
	total_packets_cloned = 0;


	const char clr[] = { 27, '[', '2', 'J', '\0' };
	const char topLeft[] = { 27, '[', '1', ';', '1', 'H','\0' };

		/* Clear screen and move to top left */
	printf("%s%s", clr, topLeft);

	printf("\nPort statistics ====================================");

	for (portid = 0; portid < RTE_MAX_ETHPORTS; portid++) {
		/* skip disabled ports */
		if ((l2fwd_enabled_port_mask & (1 << portid)) == 0)
			continue;
		// printf("\nStatistics for port %u ------------------------------"
		// 	   "\nPackets sent: %24"PRIu64
		// 	   "\nPackets received: %20"PRIu64
		// 	   "\nPackets cloned: %22"PRIu64
		// 	   "\nPackets dropped: %21"PRIu64,
		// 	   portid,
		// 	   port_statistics[portid].tx,
		// 	   port_statistics[portid].rx,
		// 	   port_statistics[portid].cloned,
		// 	   port_statistics[portid].dropped);

		total_packets_dropped += port_statistics[portid].dropped;
		total_packets_tx += port_statistics[portid].tx;
		total_packets_rx += port_statistics[portid].rx;
		total_packets_cloned += port_statistics[portid].cloned;
	}
	printf("\nAggregate statistics ==============================="
		   "\nTotal packets sent: %18"PRIu64
		   "\nTotal packets received: %14"PRIu64
		   "\nTotal packets cloned: %16"PRIu64		   
		   "\nTotal packets dropped: %15"PRIu64
		    "\nTotal packets ring0: %17"PRIu64
			"\nTotal packets ring1: %17"PRIu64
			"\nTotal packets ring2: %17"PRIu64
			 "\nTotal packets ring3: %17"PRIu64
			 "\nTotal packets ring4: %17"PRIu64
			 "\nTotal packets ring5: %17"PRIu64
			"\nTotal packets ring6: %17"PRIu64
			 "\nTotal packets ring7: %17"PRIu64,
		   total_packets_tx,
		   total_packets_rx,
		   total_packets_cloned,
		   total_packets_dropped,
		   ring_statistics[0].tx,
		   ring_statistics[1].tx,
		   ring_statistics[2].tx,
		   ring_statistics[3].tx,
		   ring_statistics[4].tx,
		   ring_statistics[5].tx,
		   ring_statistics[6].tx,
		   ring_statistics[7].tx
		   );
	printf("\n====================================================\n");

	fflush(stdout);
}

// static void
// l2fwd_mac_updating(struct rte_mbuf *m, unsigned dest_portid)
// {
// 	struct rte_ether_hdr *eth;
// 	void *tmp;

// 	eth = rte_pktmbuf_mtod(m, struct rte_ether_hdr *);

// 	/* 02:00:00:00:00:xx */
// 	tmp = &eth->d_addr.addr_bytes[0];
// 	*((uint64_t *)tmp) = 0x000000000002 + ((uint64_t)dest_portid << 40);

// 	/* src addr */
// 	rte_ether_addr_copy(&l2fwd_ports_eth_addr[dest_portid], &eth->s_addr);
// }

static void
l2fwd_mac_updating(struct rte_mbuf *m, unsigned dest_portid)
{
    struct rte_ether_hdr *eth;
    struct rte_ether_addr tmp_mac;

    eth = rte_pktmbuf_mtod(m, struct rte_ether_hdr *);

    /* Swap source and destination MAC addresses */
    rte_ether_addr_copy(&eth->s_addr, &tmp_mac);
    rte_ether_addr_copy(&eth->d_addr, &eth->s_addr);
    rte_ether_addr_copy(&tmp_mac, &eth->d_addr);

    /* Set new source MAC address */
    rte_ether_addr_copy(&l2fwd_ports_eth_addr[dest_portid], &eth->s_addr);
}


static void
l2fwd_simple_forward(struct rte_mbuf *m, unsigned portid)
{
	unsigned dst_port;
	int sent;
	struct rte_eth_dev_tx_buffer *buffer;

	dst_port = l2fwd_dst_ports[portid];
	//dst_port=16;
	//if (mac_updating)
		l2fwd_mac_updating(m, dst_port);

	buffer = tx_buffer[portid];
	sent = rte_eth_tx_buffer(portid, 0, buffer, m);
	if (sent)
		port_statistics[portid].tx += sent;
}
/* main processing loop */
/* main processing loop */
static void
l2fwd_main_loop(void)
{
	struct rte_mbuf *pkts_burst[MAX_PKT_BURST];
	struct rte_mbuf *m;
	int sent;
	unsigned lcore_id;
	uint64_t prev_tsc, diff_tsc, cur_tsc, timer_tsc;
	unsigned i, j, portid, nb_rx;
	struct lcore_queue_conf *qconf;
	const uint64_t drain_tsc = (rte_get_tsc_hz() + US_PER_S - 1) / US_PER_S *
			BURST_TX_DRAIN_US;
	struct rte_eth_dev_tx_buffer *buffer;

	prev_tsc = 0;
	timer_tsc = 0;
	unsigned dst_port;

	lcore_id = rte_lcore_id();
	qconf = &lcore_queue_conf[lcore_id];

	if (qconf->n_rx_port == 0) {
		RTE_LOG(INFO, L2FWD, "lcore %u has nothing to do\n", lcore_id);
		return;
	}

	RTE_LOG(INFO, L2FWD, "entering main loop on lcore %u\n", lcore_id);

	for (i = 0; i < qconf->n_rx_port; i++) {
		portid = qconf->rx_port_list[i];
		RTE_LOG(INFO, L2FWD, " -- lcoreid=%u portid=%u\n", lcore_id,
			portid);
	}
	int clone_idx = 0;
	while (!force_quit) {
		cur_tsc = rte_rdtsc();

		/*
		 * TX burst queue drain
		 */
		diff_tsc = cur_tsc - prev_tsc;
		if (unlikely(diff_tsc > drain_tsc)) {
			for (i = 0; i < qconf->n_rx_port; i++) {
				portid = l2fwd_dst_ports[qconf->rx_port_list[i]];
				buffer = tx_buffer[portid];
				sent = rte_eth_tx_buffer_flush(portid, 0, buffer);
				if (sent)
					port_statistics[portid].tx += sent;
			}

			/* if timer is enabled */
			if (timer_period > 0) {
				/* advance the timer */
				timer_tsc += diff_tsc;

				/* if timer has reached its timeout */
				if (unlikely(timer_tsc >= timer_period)) {
					/* do this only on master core */
					if (lcore_id == rte_get_master_lcore()) {
						print_stats();
						/* reset the timer */
						timer_tsc = 0;
					}
				}
			}

			prev_tsc = cur_tsc;
		}

		/*
		 * Read packet from RX queues
		 */
		
		for (i = 0; i < qconf->n_rx_port; i++) {
			portid = qconf->rx_port_list[i];
			nb_rx = rte_eth_rx_burst(portid, 0,
						 pkts_burst, MAX_PKT_BURST);
			port_statistics[portid].rx += nb_rx;
			
			for (j = 0; j < nb_rx; j++) {
				m = pkts_burst[j];
				rte_prefetch0(rte_pktmbuf_mtod(m, void *));
				//struct rte_ring ring_instance;
				//send_rings[0] = &ring_instance; 
			    //struct rte_ring *ring =send_rings[0];
			// if (ring != NULL) {
			  //printf("Ring at index %u: %s\n", 0, send_rings[0]);
			// } else {
			// 	printf("Error: Ring at index %u is NULL.\n", 0);
			// }
				//dst_port = l2fwd_dst_ports[portid];
				//l2fwd_mac_updating(m, dst_port);
				//print_mbuf(m);
				//int clone_idx = 0;
				if(lcore_id==2){
					handle_packet(m,send_ring0,portid,clone_idx);
				}
				else if(lcore_id==3){
					handle_packet(m,send_ring1,portid,clone_idx+1);
				}
				else if(lcore_id==4){
					handle_packet(m,send_ring2,portid,clone_idx+2);
				}
				else if(lcore_id==5){
					handle_packet(m,send_ring3,portid,clone_idx+3);
				}
				else if(lcore_id==6){
					handle_packet(m,send_ring4,portid,clone_idx+4);
				}
				else if(lcore_id==7){
					handle_packet(m,send_ring5,portid,clone_idx+5);
				}
				else if(lcore_id==8){
					handle_packet(m,send_ring6,portid,clone_idx+6);
				}
				else if(lcore_id==9){
					handle_packet(m,send_ring7,portid,clone_idx+7);
				}
				// if(lcore_id==2){
				// 	handle_packet(m,send_ring0,portid);
				// }
				// else if(lcore_id==3){
				// 	handle_packet(m,send_ring1,portid);
				// }
				// else if(lcore_id==4){
				// 	handle_packet(m,send_ring1,portid);
				// }
				// else if(lcore_id==5){
				// 	handle_packet(m,send_ring3,portid);
				// }
				// else if(lcore_id==6){
				// 	handle_packet(m,send_ring4,portid);
				// }
				// else if(lcore_id==7){
				// 	handle_packet(m,send_ring5,portid);
				// }
				// else if(lcore_id==8){
				// 	handle_packet(m,send_ring6,portid);
				// }
				// else if(lcore_id==9){
				// 	handle_packet(m,send_ring7,portid);
				// }
				clone_idx = (clone_idx + max_used_lcore) % CLONE_POOL_SIZE;
				//clone_idx = (clone_idx + 1) % CLONE_POOL_SIZE;

				// else {
				//print_mbuf(m);
				//handle_packet(m,,portid);

				//print_mbuf(m);
				// }
				l2fwd_simple_forward(m, portid);
			}
		}
	}
}

static int
l2fwd_launch_one_lcore(__rte_unused void *dummy)
{
	l2fwd_main_loop();
	return 0;
}

/* display usage */
static void
l2fwd_usage(const char *prgname)
{
	printf("%s [EAL options] -- -p PORTMASK [-q NQ]\n"
	       "  -p PORTMASK: hexadecimal bitmask of ports to configure\n"
	       "  -q NQ: number of queue (=ports) per lcore (default is 1)\n"
	       "  -T PERIOD: statistics will be refreshed each PERIOD seconds (0 to disable, 10 default, 86400 maximum)\n"
	       "  --[no-]mac-updating: Enable or disable MAC addresses updating (enabled by default)\n"
	       "      When enabled:\n"
	       "       - The source MAC address is replaced by the TX port MAC address\n"
	       "       - The destination MAC address is replaced by 02:00:00:00:00:TX_PORT_ID\n"
	       "  --portmap: Configure forwarding port pair mapping\n"
	       "	      Default: alternate port pairs\n\n",
	       prgname);
}

static int
l2fwd_parse_portmask(const char *portmask)
{
	char *end = NULL;
	unsigned long pm;

	/* parse hexadecimal string */
	pm = strtoul(portmask, &end, 16);
	if ((portmask[0] == '\0') || (end == NULL) || (*end != '\0'))
		return 0;

	return pm;
}

static int
l2fwd_parse_port_pair_config(const char *q_arg)
{
	enum fieldnames {
		FLD_PORT1 = 0,
		FLD_PORT2,
		_NUM_FLD
	};
	unsigned long int_fld[_NUM_FLD];
	const char *p, *p0 = q_arg;
	char *str_fld[_NUM_FLD];
	unsigned int size;
	char s[256];
	char *end;
	int i;

	nb_port_pair_params = 0;

	while ((p = strchr(p0, '(')) != NULL) {
		++p;
		p0 = strchr(p, ')');
		if (p0 == NULL)
			return -1;

		size = p0 - p;
		if (size >= sizeof(s))
			return -1;

		memcpy(s, p, size);
		s[size] = '\0';
		if (rte_strsplit(s, sizeof(s), str_fld,
				 _NUM_FLD, ',') != _NUM_FLD)
			return -1;
		for (i = 0; i < _NUM_FLD; i++) {
			errno = 0;
			int_fld[i] = strtoul(str_fld[i], &end, 0);
			if (errno != 0 || end == str_fld[i] ||
			    int_fld[i] >= RTE_MAX_ETHPORTS)
				return -1;
		}
		if (nb_port_pair_params >= RTE_MAX_ETHPORTS/2) {
			printf("exceeded max number of port pair params: %hu\n",
				nb_port_pair_params);
			return -1;
		}
		port_pair_params_array[nb_port_pair_params].port[0] =
				(uint16_t)int_fld[FLD_PORT1];
		port_pair_params_array[nb_port_pair_params].port[1] =
				(uint16_t)int_fld[FLD_PORT2];
		++nb_port_pair_params;
	}
	port_pair_params = port_pair_params_array;
	return 0;
}

static unsigned int
l2fwd_parse_nqueue(const char *q_arg)
{
	char *end = NULL;
	unsigned long n;

	/* parse hexadecimal string */
	n = strtoul(q_arg, &end, 10);
	if ((q_arg[0] == '\0') || (end == NULL) || (*end != '\0'))
		return 0;
	if (n == 0)
		return 0;
	if (n >= MAX_RX_QUEUE_PER_LCORE)
		return 0;

	return n;
}

static int
l2fwd_parse_timer_period(const char *q_arg)
{
	char *end = NULL;
	int n;

	/* parse number string */
	n = strtol(q_arg, &end, 10);
	if ((q_arg[0] == '\0') || (end == NULL) || (*end != '\0'))
		return -1;
	if (n >= MAX_TIMER_PERIOD)
		return -1;

	return n;
}

static const char short_options[] =
	"p:"  /* portmask */
	"q:"  /* number of queues */
	"T:"  /* timer period */
	;

#define CMD_LINE_OPT_MAC_UPDATING "mac-updating"
#define CMD_LINE_OPT_NO_MAC_UPDATING "no-mac-updating"
#define CMD_LINE_OPT_PORTMAP_CONFIG "portmap"

enum {
	/* long options mapped to a short option */

	/* first long only option value must be >= 256, so that we won't
	 * conflict with short options */
	CMD_LINE_OPT_MIN_NUM = 256,
	CMD_LINE_OPT_PORTMAP_NUM,
};

static const struct option lgopts[] = {
	{ CMD_LINE_OPT_MAC_UPDATING, no_argument, &mac_updating, 1},
	{ CMD_LINE_OPT_NO_MAC_UPDATING, no_argument, &mac_updating, 0},
	{ CMD_LINE_OPT_PORTMAP_CONFIG, 1, 0, CMD_LINE_OPT_PORTMAP_NUM},
	{NULL, 0, 0, 0}
};

/* Parse the argument given in the command line of the application */
static int
l2fwd_parse_args(int argc, char **argv)
{
	int opt, ret, timer_secs;
	char **argvopt;
	int option_index;
	char *prgname = argv[0];

	argvopt = argv;
	port_pair_params = NULL;

	while ((opt = getopt_long(argc, argvopt, short_options,
				  lgopts, &option_index)) != EOF) {

		switch (opt) {
		/* portmask */
		case 'p':
			l2fwd_enabled_port_mask = l2fwd_parse_portmask(optarg);
			if (l2fwd_enabled_port_mask == 0) {
				printf("invalid portmask\n");
				l2fwd_usage(prgname);
				return -1;
			}
			break;

		/* nqueue */
		case 'q':
			l2fwd_rx_queue_per_lcore = l2fwd_parse_nqueue(optarg);
			if (l2fwd_rx_queue_per_lcore == 0) {
				printf("invalid queue number\n");
				l2fwd_usage(prgname);
				return -1;
			}
			break;

		/* timer period */
		case 'T':
			timer_secs = l2fwd_parse_timer_period(optarg);
			if (timer_secs < 0) {
				printf("invalid timer period\n");
				l2fwd_usage(prgname);
				return -1;
			}
			timer_period = timer_secs;
			break;

		/* long options */
		case CMD_LINE_OPT_PORTMAP_NUM:
			ret = l2fwd_parse_port_pair_config(optarg);
			if (ret) {
				fprintf(stderr, "Invalid config\n");
				l2fwd_usage(prgname);
				return -1;
			}
			break;

		default:
			l2fwd_usage(prgname);
			return -1;
		}
	}

	if (optind >= 0)
		argv[optind-1] = prgname;

	ret = optind-1;
	optind = 1; /* reset getopt lib */
	return ret;
}

/*
 * Check port pair config with enabled port mask,
 * and for valid port pair combinations.
 */
static int
check_port_pair_config(void)
{
	uint32_t port_pair_config_mask = 0;
	uint32_t port_pair_mask = 0;
	uint16_t index, i, portid;

	for (index = 0; index < nb_port_pair_params; index++) {
		port_pair_mask = 0;

		for (i = 0; i < NUM_PORTS; i++)  {
			portid = port_pair_params[index].port[i];
			if ((l2fwd_enabled_port_mask & (1 << portid)) == 0) {
				printf("port %u is not enabled in port mask\n",
				       portid);
				return -1;
			}
			if (!rte_eth_dev_is_valid_port(portid)) {
				printf("port %u is not present on the board\n",
				       portid);
				return -1;
			}

			port_pair_mask |= 1 << portid;
		}

		if (port_pair_config_mask & port_pair_mask) {
			printf("port %u is used in other port pairs\n", portid);
			return -1;
		}
		port_pair_config_mask |= port_pair_mask;
	}

	l2fwd_enabled_port_mask &= port_pair_config_mask;

	return 0;
}

/* Check the link status of all ports in up to 9s, and print them finally */
static void
check_all_ports_link_status(uint32_t port_mask)
{
#define CHECK_INTERVAL 100 /* 100ms */
#define MAX_CHECK_TIME 90 /* 9s (90 * 100ms) in total */
	uint16_t portid;
	uint8_t count, all_ports_up, print_flag = 0;
	struct rte_eth_link link;
	int ret;

	printf("\nChecking link status");
	fflush(stdout);
	for (count = 0; count <= MAX_CHECK_TIME; count++) {
		if (force_quit)
			return;
		all_ports_up = 1;
		RTE_ETH_FOREACH_DEV(portid) {
			if (force_quit)
				return;
			if ((port_mask & (1 << portid)) == 0)
				continue;
			memset(&link, 0, sizeof(link));
			ret = rte_eth_link_get_nowait(portid, &link);
			if (ret < 0) {
				all_ports_up = 0;
				if (print_flag == 1)
					printf("Port %u link get failed: %s\n",
						portid, rte_strerror(-ret));
				continue;
			}
			/* print link status if flag set */
			if (print_flag == 1) {
				if (link.link_status)
					printf(
					"Port%d Link Up. Speed %u Mbps - %s\n",
						portid, link.link_speed,
				(link.link_duplex == ETH_LINK_FULL_DUPLEX) ?
					("full-duplex") : ("half-duplex"));
				else
					printf("Port %d Link Down\n", portid);
				continue;
			}
			/* clear all_ports_up flag if any link down */
			if (link.link_status == ETH_LINK_DOWN) {
				all_ports_up = 0;
				break;
			}
		}
		/* after finally printing all link status, get out */
		if (print_flag == 1)
			break;

		if (all_ports_up == 0) {
			printf(".");
			fflush(stdout);
			rte_delay_ms(CHECK_INTERVAL);
		}

		/* set the print_flag if all ports up or timeout */
		if (all_ports_up == 1 || count == (MAX_CHECK_TIME - 1)) {
			print_flag = 1;
			printf("done\n");
		}
	}
}

static void
signal_handler(int signum)
{
	if (signum == SIGINT || signum == SIGTERM) {
		printf("\n\nSignal %d received, preparing to exit...\n",
				signum);
		force_quit = true;
	}
}

int
main(int argc, char **argv)
{
	struct lcore_queue_conf *qconf;
	int ret;
	uint16_t nb_ports;
	uint16_t nb_ports_available = 0;
	uint16_t portid, last_port;
	unsigned lcore_id, rx_lcore_id;
	unsigned nb_ports_in_mask = 0;
	unsigned int nb_lcores = 0;
	unsigned int nb_mbufs;

	/* init EAL */
	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Invalid EAL arguments\n");
	argc -= ret;
	argv += ret;

	force_quit = false;
	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);

	/* parse application arguments (after the EAL ones) */
	ret = l2fwd_parse_args(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Invalid L2FWD arguments\n");

	printf("MAC updating %s\n", mac_updating ? "enabled" : "disabled");

	/* convert to number of cycles */
	timer_period *= rte_get_timer_hz();

	nb_ports = rte_eth_dev_count_avail();
	if (nb_ports == 0)
		rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");

	if (port_pair_params != NULL) {
		if (check_port_pair_config() < 0)
			rte_exit(EXIT_FAILURE, "Invalid port pair config\n");
	}

	/* check port mask to possible port mask */
	// if (l2fwd_enabled_port_mask & ~((1 << nb_ports) - 1))
	// 	rte_exit(EXIT_FAILURE, "Invalid portmask; possible (0x%x)\n",
	// 		(1 << nb_ports) - 1);

	/* reset l2fwd_dst_ports */
	for (portid = 0; portid < RTE_MAX_ETHPORTS; portid++)
		l2fwd_dst_ports[portid] = 0;
	last_port = 0;

	/* populate destination port details */
	if (port_pair_params != NULL) {
		uint16_t idx, p;

		for (idx = 0; idx < (nb_port_pair_params << 1); idx++) {
			p = idx & 1;
			portid = port_pair_params[idx >> 1].port[p];
			l2fwd_dst_ports[portid] =
				port_pair_params[idx >> 1].port[p ^ 1];
		}
	} else {
		RTE_ETH_FOREACH_DEV(portid) {
			/* skip ports that are not enabled */
			if ((l2fwd_enabled_port_mask & (1 << portid)) == 0)
				continue;

			if (nb_ports_in_mask % 2) {
				l2fwd_dst_ports[portid] = portid;
				l2fwd_dst_ports[last_port] = last_port;
			} else {
				last_port = portid;
			}

			nb_ports_in_mask++;
		}
		if (nb_ports_in_mask % 2) {
			printf("Notice: odd number of ports in portmask.\n");
			l2fwd_dst_ports[last_port] = last_port;
		}
	}

	rx_lcore_id = 0;
	qconf = NULL;

	/* Initialize the port/queue configuration of each logical core */
	RTE_ETH_FOREACH_DEV(portid) {
		/* skip ports that are not enabled */
		if ((l2fwd_enabled_port_mask & (1 << portid)) == 0)
			continue;

		/* get the lcore_id for this port */
		while (rte_lcore_is_enabled(rx_lcore_id) == 0 ||
		       lcore_queue_conf[rx_lcore_id].n_rx_port ==
		       l2fwd_rx_queue_per_lcore) {
			rx_lcore_id++;
			
			if (rx_lcore_id >= RTE_MAX_LCORE)
				rte_exit(EXIT_FAILURE, "Not enough cores\n");
		}

		if (qconf != &lcore_queue_conf[rx_lcore_id]) {
			/* Assigned a new logical core in the loop above. */
			qconf = &lcore_queue_conf[rx_lcore_id];
			max_used_lcore++;
			nb_lcores++;
		}

		qconf->rx_port_list[qconf->n_rx_port] = portid;
		qconf->n_rx_port++;
		printf("Lcore %u: RX port %u TX port %u\n", rx_lcore_id,
		       portid, l2fwd_dst_ports[portid]);
	}

printf("Max used lcore: %u\n", max_used_lcore);

// rx_lcore_id = 0;
// qconf = NULL;

// /* Initialize the port/queue configuration of each logical core */
// RTE_ETH_FOREACH_DEV(portid) {
//     /* Skip ports that are not enabled */
//     if ((l2fwd_enabled_port_mask & (1 << portid)) == 0)
//         continue;

//     /* Get the lcore_id for this port */
//     while (rte_lcore_is_enabled(rx_lcore_id) == 0 ||
//            lcore_queue_conf[rx_lcore_id].n_rx_port ==
//            l2fwd_rx_queue_per_lcore) {
//         rx_lcore_id++;
//         if (rx_lcore_id >= RTE_MAX_LCORE)
//             rte_exit(EXIT_FAILURE, "Not enough cores\n");
//     }

//     if (qconf != &lcore_queue_conf[rx_lcore_id]) {
//         /* Assigned a new logical core in the loop above. */
//         qconf = &lcore_queue_conf[rx_lcore_id];
//         nb_lcores++;

//         /* Add the lcore_id to the global list */
//         used_lcore_ids[num_used_lcores++] = rx_lcore_id;
//     }

//     qconf->rx_port_list[qconf->n_rx_port] = portid;
//     qconf->n_rx_port++;
//     printf("Lcore %u: RX port %u TX port %u\n", rx_lcore_id,
//            portid, l2fwd_dst_ports[portid]);
// }


	// nb_mbufs = RTE_MAX(nb_ports * (nb_rxd + nb_txd + MAX_PKT_BURST +
	// 	nb_lcores * MEMPOOL_CACHE_SIZE), 8192U);
	nb_mbufs = 100000U;
	printf("Skipping disabled port %u\n", nb_mbufs);


	/* create the mbuf pool */
	l2fwd_pktmbuf_pool = rte_pktmbuf_pool_create("mbuf_pool", nb_mbufs,
		MEMPOOL_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE,
		rte_socket_id());
	if (l2fwd_pktmbuf_pool == NULL)
		rte_exit(EXIT_FAILURE, "Cannot init mbuf pool\n");

    my_clone_pool = rte_pktmbuf_pool_create("my_clone_pool", CLONE_POOL_SIZE, MEMPOOL_CACHE_SIZE,
            0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

    if (my_clone_pool == NULL)
        rte_exit(EXIT_FAILURE, "Cannot init my clone mbuf pool\n");

    // Pre-allocate mbufs from the clone pool
    for (int i = 0; i < CLONE_POOL_SIZE; i++) {
        clone_mbufs[i] = rte_pktmbuf_alloc(my_clone_pool);
        if (clone_mbufs[i] == NULL) {
            rte_exit(EXIT_FAILURE, "Failed to pre-allocate mbufs for cloning\n");
        }
    }
	

	// #define RING_F_SP_ENQ 0x0001 /**< The default enqueue is "single-producer". */
	// #define RING_F_SC_DEQ 0x0002 /**< The default dequeue is "single-consumer". */
	// #define RING_F_MP_RTS_ENQ 0x0008 /**< The default enqueue is "MP RTS". */
	// #define RING_F_MC_RTS_DEQ 0x0010 /**< The default dequeue is "MC RTS". */

	// #define RING_F_MP_HTS_ENQ 0x0020 /**< The default enqueue is "MP HTS". */
	// #define RING_F_MC_HTS_DEQ 0x0040 /**< The default dequeue is "MC HTS". */
	//const unsigned flags = 0x0001 | 0x0002;
	//const unsigned flags;//= 0x0020; //bestfirsttest
	const unsigned flags= 0x0020;
	const unsigned ring_size = 2097152*8*16;
    send_ring0 = rte_ring_create("ring0", ring_size, rte_socket_id(), flags);
	send_ring1 = rte_ring_create("ring1", ring_size, rte_socket_id(), flags);
    send_ring2 = rte_ring_create("ring2", ring_size, rte_socket_id(),flags);  
    send_ring3 = rte_ring_create("ring3", ring_size, rte_socket_id(), flags);
	send_ring4 = rte_ring_create("ring4", ring_size, rte_socket_id(), flags);
    send_ring5 = rte_ring_create("ring5", ring_size, rte_socket_id(),flags);  
	send_ring6 = rte_ring_create("ring6", ring_size, rte_socket_id(), flags);
    send_ring7 = rte_ring_create("ring7", ring_size, rte_socket_id(),flags);  
    // struct rte_ring *send_rings[max_used_lcore];
    // char ring_name[32];

    // for (unsigned int lcore_id1 = 0; lcore_id1 < max_used_lcore; lcore_id1++) {
    //     // Create unique name for each ring buffer
    //     snprintf(ring_name, sizeof(ring_name), "send_ring_%u", lcore_id1);
        
    //     // Create the ring buffer
    //     send_rings[lcore_id1] = rte_ring_create(ring_name, ring_size, rte_socket_id(), flags);
        
    //     if (send_rings[lcore_id1] == NULL) {
    //         rte_exit(EXIT_FAILURE, "Error creating ring buffer for lcore %u\n", lcore_id1);
    //     } else {
    //         printf("Ring buffer %s created successfully\n", ring_name);
    //     }
    // }

    // message_pool = rte_mempool_create('MSG_POOL0', pool_size,
    //         STR_TOKEN_SIZE, pool_cache, priv_data_sz,
    //         NULL, NULL, NULL, NULL,
    //         rte_socket_id(), flags);

    // message_pool1 = rte_mempool_create('MSG_POOL1', pool_size,
    //         STR_TOKEN_SIZE, pool_cache, priv_data_sz,
    //         NULL, NULL, NULL, NULL,
    //         rte_socket_id(), flags);

	/* Initialize the hash tables */

	
	// populate_hash_table("teid_qfi_entries.txt");
	// populate_ul_hash_table("ul_teid_entries.txt");
	populate_teid_qfi_table("/home/dv/mohsen/dpdk20/dpdk/examples/gnb/config_table1_64000_0_table2_10.0.0.0_10.0.250.0_0.txt");
	//populate_ul_teid_table("/home/dv/mohsen/dpdk20/dpdk/examples/gnb/config_table1_64000_0_table2_10.0.0.0_10.0.250.0_0.txt");

    // Print total number of entries in each hash table
    if (teid_qfi_table != NULL) {
        printf("Total number of entries in teid_qfi_table: %u\n", rte_hash_count(teid_qfi_table));
    } else {
        printf("teid_qfi_table not initialized.\n");
    }

    // if (ul_teid_table != NULL) {
    //     printf("Total number of entries in ul_teid_table: %u\n", rte_hash_count(ul_teid_table));
    // } else {
    //     printf("ul_teid_table not initialized.\n");
    // }

    // Perform lookup tests with specific inputs
    // uint32_t teid_to_lookup = 3; // Example TEID to lookup
    // const char *ip_to_lookup = "10.0.0.3"; // Example IP to lookup

    // printf("\nPerforming lookups for testing...\n");
    // lookup_teid_in_teid_qfi_table(teid_to_lookup);
    // lookup_ip_in_ul_teid_table(ip_to_lookup);

	/* Initialise each port */
	RTE_ETH_FOREACH_DEV(portid) {
		struct rte_eth_rxconf rxq_conf;
		struct rte_eth_txconf txq_conf;
		struct rte_eth_conf local_port_conf = port_conf;
		struct rte_eth_dev_info dev_info;

		/* skip ports that are not enabled */
		if ((l2fwd_enabled_port_mask & (1 << portid)) == 0) {
			printf("Skipping disabled port %u\n", portid);
			continue;
		}
		nb_ports_available++;

		/* init port */
		printf("Initializing port %u... ", portid);
		fflush(stdout);

		ret = rte_eth_dev_info_get(portid, &dev_info);
		if (ret != 0)
			rte_exit(EXIT_FAILURE,
				"Error during getting device (port %u) info: %s\n",
				portid, strerror(-ret));

		if (dev_info.tx_offload_capa & DEV_TX_OFFLOAD_MBUF_FAST_FREE)
			local_port_conf.txmode.offloads |=
				DEV_TX_OFFLOAD_MBUF_FAST_FREE;
		ret = rte_eth_dev_configure(portid, 1, 1, &local_port_conf);
		if (ret < 0)
			rte_exit(EXIT_FAILURE, "Cannot configure device: err=%d, port=%u\n",
				  ret, portid);

		ret = rte_eth_dev_adjust_nb_rx_tx_desc(portid, &nb_rxd,
						       &nb_txd);
		if (ret < 0)
			rte_exit(EXIT_FAILURE,
				 "Cannot adjust number of descriptors: err=%d, port=%u\n",
				 ret, portid);

		ret = rte_eth_macaddr_get(portid,
					  &l2fwd_ports_eth_addr[portid]);
		if (ret < 0)
			rte_exit(EXIT_FAILURE,
				 "Cannot get MAC address: err=%d, port=%u\n",
				 ret, portid);

		/* init one RX queue */
		fflush(stdout);
		rxq_conf = dev_info.default_rxconf;
		rxq_conf.offloads = local_port_conf.rxmode.offloads;
		ret = rte_eth_rx_queue_setup(portid, 0, nb_rxd,
					     rte_eth_dev_socket_id(portid),
					     &rxq_conf,
					     l2fwd_pktmbuf_pool);
		if (ret < 0)
			rte_exit(EXIT_FAILURE, "rte_eth_rx_queue_setup:err=%d, port=%u\n",
				  ret, portid);

		/* init one TX queue on each port */
		fflush(stdout);
		txq_conf = dev_info.default_txconf;
		txq_conf.offloads = local_port_conf.txmode.offloads;
		ret = rte_eth_tx_queue_setup(portid, 0, nb_txd,
				rte_eth_dev_socket_id(portid),
				&txq_conf);
		if (ret < 0)
			rte_exit(EXIT_FAILURE, "rte_eth_tx_queue_setup:err=%d, port=%u\n",
				ret, portid);

		/* Initialize TX buffers */
		tx_buffer[portid] = rte_zmalloc_socket("tx_buffer",
				RTE_ETH_TX_BUFFER_SIZE(MAX_PKT_BURST), 0,
				rte_eth_dev_socket_id(portid));
		if (tx_buffer[portid] == NULL)
			rte_exit(EXIT_FAILURE, "Cannot allocate buffer for tx on port %u\n",
					portid);

		rte_eth_tx_buffer_init(tx_buffer[portid], MAX_PKT_BURST);

		ret = rte_eth_tx_buffer_set_err_callback(tx_buffer[portid],
				rte_eth_tx_buffer_count_callback,
				&port_statistics[portid].dropped);
		if (ret < 0)
			rte_exit(EXIT_FAILURE,
			"Cannot set error callback for tx buffer on port %u\n",
				 portid);

		ret = rte_eth_dev_set_ptypes(portid, RTE_PTYPE_UNKNOWN, NULL,
					     0);
		if (ret < 0)
			printf("Port %u, Failed to disable Ptype parsing\n",
					portid);
		/* Start device */
		ret = rte_eth_dev_start(portid);
		if (ret < 0)
			rte_exit(EXIT_FAILURE, "rte_eth_dev_start:err=%d, port=%u\n",
				  ret, portid);

		printf("done: \n");

		ret = rte_eth_promiscuous_enable(portid);
		if (ret != 0)
			rte_exit(EXIT_FAILURE,
				 "rte_eth_promiscuous_enable:err=%s, port=%u\n",
				 rte_strerror(-ret), portid);

		printf("Port %u, MAC address: %02X:%02X:%02X:%02X:%02X:%02X\n\n",
				portid,
				l2fwd_ports_eth_addr[portid].addr_bytes[0],
				l2fwd_ports_eth_addr[portid].addr_bytes[1],
				l2fwd_ports_eth_addr[portid].addr_bytes[2],
				l2fwd_ports_eth_addr[portid].addr_bytes[3],
				l2fwd_ports_eth_addr[portid].addr_bytes[4],
				l2fwd_ports_eth_addr[portid].addr_bytes[5]);

		/* initialize port stats */
		memset(&port_statistics, 0, sizeof(port_statistics));
	}

	if (!nb_ports_available) {
		rte_exit(EXIT_FAILURE,
			"All available ports are disabled. Please set portmask.\n");
	}

	check_all_ports_link_status(l2fwd_enabled_port_mask);

	ret = 0;
	/* launch per-lcore init on every lcore */
	rte_eal_mp_remote_launch(l2fwd_launch_one_lcore, NULL, CALL_MASTER);
	//rte_eal_mp_remote_launch(l2fwd_launch_one_lcore, NULL, SKIP_MASTER);
	RTE_LCORE_FOREACH_SLAVE(lcore_id) {
		if (rte_eal_wait_lcore(lcore_id) < 0) {
			ret = -1;
			break;
		}
	}

	RTE_ETH_FOREACH_DEV(portid) {
		if ((l2fwd_enabled_port_mask & (1 << portid)) == 0)
			continue;
		printf("Closing port %d...", portid);
		rte_eth_dev_stop(portid);
		rte_eth_dev_close(portid);
		printf(" Done\n");
	}
	printf("Bye...\n");

	return ret;
}
