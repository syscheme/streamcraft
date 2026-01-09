#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_udp.h>
#include <rte_rte.h>

// RTP header structure
struct rtp_hdr {
    uint8_t version:2;
    uint8_t padding:1;
    uint8_t extension:1;
    uint8_t cc:4; // CSRC count
    uint8_t m:1;  // Marker bit
    uint8_t pt;   // Payload type
    uint16_t seq; // Sequence number
    uint32_t timestamp;
    uint32_t ssrc;
};

int main(int argc, char **argv)
{
    struct rte_mempool *mbuf_pool = NULL;
    struct rte_eth_dev_info dev_info;
    struct rte_eth_rxconf rx_conf;
    struct rte_eth_txconf tx_conf;
    struct rtp_hdr *rtp_hdr;
    struct rte_mbuf *tx_pkt;
    uint16_t port_id;
    uint8_t mac_addr[6];
    uint8_t ip_addr[4];
    uint16_t dst_port;

    // Initialize the EAL
    if (rte_eal_init(argc, argv) < 0) {
        rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");
    }

    // Get the first available port ID
    port_id = rte_eth_dev_find_free();

    // Get device information
    rte_eth_dev_info_get(port_id, &dev_info);

    // Create a mempool for storing packets
    mbuf_pool = rte_pktmbuf_pool_create("mbuf_pool", 1024, 16, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

    // Configure the RX queue
    memset(&rx_conf, 0, sizeof(rx_conf));
    rx_conf.rx_thresh.pthresh = 8;
    rx_conf.rx_thresh.hthresh = 0;
    rx_conf.rx_thresh.wthresh = 0;

    // Configure the TX queue
    memset(&tx_conf, 0, sizeof(tx_conf));
    tx_conf.tx_thresh.pthresh = 8;
    tx_conf.tx_thresh.hthresh = 0;
    tx_conf.tx_thresh.wthresh = 0;

    // Start the port
    if (rte_eth_dev_start(port_id, 1, 1, &rx_conf, &tx_conf) < 0) {
        rte_exit(EXIT_FAILURE, "Error starting port\n");
    }

    // Get the MAC address of the port
    rte_eth_macaddr_get(port_id, mac_addr);

    // Create a packet and fill in the RTP header
    tx_pkt = rte_pktmbuf_alloc(mbuf_pool);
    if (!tx_pkt) {
        rte_exit(EXIT_FAILURE, "Failed to allocate packet buffer\n");
    }

    // Set up the RTP header
    rtp_hdr = rte_pktmbuf_mtod(tx_pkt, struct rtp_hdr *);
    rtp_hdr->version = 2;
    rtp_hdr->padding = 0;
    rtp_hdr->extension = 0;
    rtp_hdr->cc = 0;
    rtp_hdr->m = 0;
    rtp_hdr->pt = 100; // Example payload type
    rtp_hdr->seq = 1234;
    rtp_hdr->timestamp = 0x12345678;
    rtp_hdr->ssrc = 0x87654321;

    // Fill in the Ethernet header
    struct rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(tx_pkt, struct rte_ether_hdr *);
    rte_ether_addr_set_all_ones(&eth_hdr->d_addr); // Destination MAC (broadcast)
    rte_ether_addr_copy(mac_addr, &eth_hdr->s_addr); // Source MAC

    // Fill in the IP header
    struct rte_ipv4_hdr *ip_hdr = (struct rte_ipv4_hdr *)((char *)rtp_hdr + sizeof(struct rtp_hdr));
    ip_hdr->next_proto_id = IPPROTO_UDP;
    ip_hdr->total_length = htons(sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + sizeof(struct rtp_hdr));
    ip_hdr->time_to_live = 255;
    ip_hdr->version_ihl = 0x45;
    ip_hdr->src_addr = 0x7F000001; // Example source IP
    ip_hdr->dst_addr = 0x7F000001; // Example destination IP

    // Fill in the UDP header
    struct rte_udp_hdr *udp_hdr = (struct rte_udp_hdr *)((char *)ip_hdr + sizeof(struct rte_ipv4_hdr));
    udp_hdr->src_port = htons(1234); // Example source port
    udp_hdr->dst_port = htons(dst_port); // Destination port
    udp_hdr->dgram_len = htons(sizeof(struct rte_udp_hdr) + sizeof(struct rtp_hdr));

    // Calculate IP checksum
    ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

    // Calculate UDP checksum
    udp_hdr->chksum = rte_udp_cksum(ip_hdr, udp_hdr, sizeof(struct rtp_hdr));

    // Send the packet
    rte_eth_tx_buffer(port_id, 0, &tx_pkt, 1);

    // Cleanup
    rte_eth_dev_stop(port_id);
    rte_eth_dev_close(port_id);
    rte_pktmbuf_pool_free(mbuf_pool);

    return 0;
}

// usage: ./$0 -l 0-3 -- -p 0x1 
// -l 0-3 表示使用 CPU 核心 0 至 3，-p 0x1 表示使用第一个可用的物理端口

// make -C /path/to/DPDK/examples/dpdk_app