#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_malloc.h>
#include <rte_launch.h>
#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_per_lcore.h>
#include <rte_ring.h>
#include <rte_mempool.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_tcp.h>
#include <rte_udp.h>
#include <rte_string_fns.h>
#include <rte_malloc.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>

#define RX_RING_SIZE 1024
#define TX_RING_SIZE 1024
#define MBUF_SIZE (2 * 1024)
#define NB_MBUF (8191 * RX_RING_SIZE)

#define FRAME_SIZE (32 * 1024 * 1024)  // 32MB 视频帧大小

static const char *port_conf = "all";

static struct rte_mempool *mbuf_pool;
static struct rte_eth_dev_info dev_info;
static struct rte_eth_txconf tx_conf;
static struct rte_eth_rxconf rx_conf;

static int
init_port(uint16_t port_id) {
    struct rte_eth_conf port_conf = { .rxmode = { .max_rx_pkt_len = ETHER_MAX_LEN } };
    int ret;

    // 初始化端口
    ret = rte_eth_dev_configure(port_id, 1, 1, &port_conf);
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "rte_eth_dev_configure: err=%d, port=%u\n", ret, port_id);
    }

    // 设置收发队列
    ret = rte_eth_rx_queue_setup(port_id, 0, RX_RING_SIZE, rte_eth_dev_socket_id(port_id), &rx_conf, mbuf_pool);
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "rte_eth_rx_queue_setup: err=%d, port=%u\n", ret, port_id);
    }

    ret = rte_eth_tx_queue_setup(port_id, 0, TX_RING_SIZE, rte_eth_dev_socket_id(port_id), &tx_conf);
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "rte_eth_tx_queue_setup: err=%d, port=%u\n", ret, port_id);
    }

    // 启动端口
    ret = rte_eth_dev_start(port_id);
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "rte_eth_dev_start: err=%d, port=%u\n", ret, port_id);
    }

    return 0;
}

static int
main_loop(__attribute__((unused)) void *dummy) {
    uint16_t port_id = 0;
    uint16_t nb_rx;
    struct rte_mbuf *pkts_burst[RX_RING_SIZE];
    int ret;
    const char *shm_path = "/dev/shm/video_frame";
    int shm_fd;
    void *shm_ptr;
    char eal_huge_dir[PATH_MAX];

    // 获取 EAL 配置的巨页目录
    if (rte_eal_get_hugepage_info(eal_huge_dir, sizeof(eal_huge_dir)) < 0) {
        rte_exit(EXIT_FAILURE, "Failed to get hugepage directory\n");
    }

    // 打开共享内存文件
    shm_fd = open(shm_path, O_RDONLY);
    if (shm_fd == -1) {
        perror("open");
        return -1;
    }

    // 映射到内存
    shm_ptr = mmap(NULL, FRAME_SIZE, PROT_READ, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED) {
        perror("mmap");
        close(shm_fd);
        return -1;
    }

    while (1) {
        // 接收数据包
        nb_rx = rte_eth_rx_burst(port_id, 0, pkts_burst, RX_RING_SIZE);
        if (nb_rx > 0) {
            // 处理接收到的数据包
            for (uint16_t i = 0; i < nb_rx; i++) {
                // 这里可以添加对数据包的处理逻辑
                rte_pktmbuf_free(pkts_burst[i]);
            }
        }

        // 读取共享内存中的数据并发送
        struct rte_mbuf *tx_mbuf = rte_pktmbuf_alloc(mbuf_pool);
        if (tx_mbuf == NULL) {
            continue;
        }

        // 判断业务数据是否在相同的巨页目录中
        if (strncmp(eal_huge_dir, "/dev/shm", PATH_MAX) == 0) {
            // 直接设置 buf_addr 指针
            tx_mbuf->buf_addr = (uintptr_t)shm_ptr;
            tx_mbuf->data_off = 0;
            tx_mbuf->data_len = FRAME_SIZE;
            tx_mbuf->pkt_len = FRAME_SIZE;
        } else {
            // 使用 rte_memcpy 复制数据
            rte_memcpy(rte_pktmbuf_mtod(tx_mbuf, void *), shm_ptr, FRAME_SIZE);
            tx_mbuf->data_len = FRAME_SIZE;
            tx_mbuf->pkt_len = FRAME_SIZE;
        }

        // 发送数据包
        ret = rte_eth_tx_burst(port_id, 0, &tx_mbuf, 1);
        if (ret < 0) {
            rte_exit(EXIT_FAILURE, "rte_eth_tx_burst: err=%d, port=%u\n", ret, port_id);
        }
    }

    // 解除映射
    if (munmap(shm_ptr, FRAME_SIZE) == -1) {
        perror("munmap");
    }

    // 关闭文件
    close(shm_fd);

    return 0;
}

int
main(int argc, char **argv) {
    uint16_t nb_ports;
    int ret;

    // 初始化 EAL
    ret = rte_eal_init(argc, argv);
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "Invalid EAL arguments\n");
    }
    argc -= ret;
    argv += ret;

    // 获取端口数量
    nb_ports = rte_eth_dev_count_avail();
    if (nb_ports == 0) {
        rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
    }

    // 创建 mbuf 池
    mbuf_pool = rte_pktmbuf_pool_create("MBUF_POOL", NB_MBUF, 32, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
    if (mbuf_pool == NULL) {
        rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");
    }

    // 初始化端口
    for (uint16_t port_id = 0; port_id < nb_ports; port_id++) {
        init_port(port_id);
    }

    // 启动主循环
    rte_eal_mp_remote_launch(main_loop, NULL, CALL_MASTER);
    rte_eal_mp_wait_lcore();

    return 0;
}

/*

获取 EAL 配置的巨页目录：
使用 rte_eal_get_hugepage_info 函数获取 EAL 配置的巨页目录。
将获取到的目录路径存储在 eal_huge_dir 变量中。
判断业务数据是否在相同的巨页目录中：
在主循环中，通过 strncmp 函数比较 eal_huge_dir 和 /dev/shm。
如果相同，则直接设置 tx_mbuf->buf_addr 指针。
如果不同，则使用 rte_memcpy 复制数据。
编译和运行
编译并运行该 DPDK 应用程序：
gcc -o dpdk_app dpdk_app.c -I/usr/local/include -L/usr/local/lib -lrte_eal -lrte_ethdev -lrte_mbuf -lrte_mempool -Wl,--no-as-needed -lrt -lm -ldl -lpthread
sudo ./dpdk_app -- -l 0-3 -n 4 --huge-dir /dev/shm

通过这种方式，你可以在程序内部动态地判断业务数据是否在相同的巨页目录中，并选择最高效的方式来处理数据。这样可以避免不必要的数据复制，提高性能。


====================
在多 NUMA（Non-Uniform Memory Access）系统中，为了提高 DPDK 网卡对 /dev/shm 中数据的访问效率，需要确保巨页和共享内存文件位于与网卡相同的 NUMA 节点上。这样可以减少跨 NUMA 节点的数据传输延迟，从而提高性能。

多 NUMA 系统中的优化步骤
确定网卡所在的 NUMA 节点：
首先，你需要确定你的 DPDK 网卡所在的 NUMA 节点。你可以使用 lspci -v 或 ethtool -i <interface> 命令来查看网卡的 PCI 信息，并找到其对应的 NUMA 节点。
配置巨页到特定的 NUMA 节点：
通过内核参数将巨页分配到特定的 NUMA 节点。例如，假设你的网卡在 NUMA 节点 0 上：
    echo 2048 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages

挂载 hugetlbfs 到特定的 NUMA 节点：
将 hugetlbfs 挂载到特定的 NUMA 节点。你可以使用 numactl 工具来控制挂载点的位置。例如：
    sudo umount /dev/shm
    sudo numactl --membind=0 mount -t hugetlbfs nodev /dev/shm
DPDK 应用程序绑定到特定的 NUMA 节点：
在启动 DPDK 应用程序时，确保它绑定到与网卡相同的 NUMA 节点。可以通过 EAL 参数来指定 CPU 核心掩码和内存通道数，并使用 --socket-mem 参数来分配内存到特定的 NUMA 节点。
    sudo ./build/app/testpmd -l 0-3 -n 4 --socket-mem 0,1024 --huge-dir /dev/shm -- -i
在这个例子中，--socket-mem 0,1024 表示在 NUMA 节点 0 上分配 1024 MB 的内存。
业务逻辑绑定到特定的 NUMA 节点：
确保你的业务逻辑也绑定到与网卡相同的 NUMA 节点。你可以使用 numactl 来启动你的业务逻辑进程。例如：
    numactl --membind=0 --cpunodebind=0 ./your_business_logic_app
示例
假设你的网卡在 NUMA 节点 0 上，以下是一个完整的示例：

1. 配置巨页
    # 配置 2MB 巨页到 NUMA 节点 0
    echo 2048 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages
2. 挂载 hugetlbfs 到 NUMA 节点 0
    sudo umount /dev/shm
    sudo numactl --membind=0 mount -t hugetlbfs nodev /dev/shm
3. 启动 DPDK 应用程序
    sudo ./build/app/testpmd -l 0-3 -n 4 --socket-mem 0,1024 --huge-dir /dev/shm -- -i
4. 启动业务逻辑
    numactl --membind=0 --cpunodebind=0 ./your_business_logic_app

通过将巨页、共享内存文件和应用程序绑定到与网卡相同的 NUMA 节点，可以显著减少跨 NUMA 节点的数据传输延迟，从而提高 DPDK 网卡对 /dev/shm 中数据的访问效率。以下是关键步骤的总结：

- 确定网卡所在的 NUMA 节点。
- 配置巨页到该 NUMA 节点。
- 挂载 hugetlbfs 到该 NUMA 节点。
- 启动 DPDK 应用程序并绑定到该 NUMA 节点。
-  启动业务逻辑并绑定到该 NUMA 节点。
通过这些步骤，你可以确保数据在同一个 NUMA 节点内进行处理，从而提高整体性能。

编写一个简单的 C 程序假设业务逻辑，将处理后的视频帧写入 /dev/shm 中的一个文件, your_business_logic_app.cpp:
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#define FRAME_SIZE (32 * 1024 * 1024)  // 32MB 视频帧大小

int main() {
    const char *shm_path = "/dev/shm/video_frame";
    int shm_fd;
    void *shm_ptr;

    // 创建并打开共享内存文件
    shm_fd = open(shm_path, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("open");
        exit(EXIT_FAILURE);
    }

    // 分配足够大的空间
    if (ftruncate(shm_fd, FRAME_SIZE) == -1) {
        perror("ftruncate");
        close(shm_fd);
        unlink(shm_path);
        exit(EXIT_FAILURE);
    }

    // 映射到内存
    shm_ptr = mmap(NULL, FRAME_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED) {
        perror("mmap");
        close(shm_fd);
        unlink(shm_path);
        exit(EXIT_FAILURE);
    }

    // 假设这里有一些视频处理逻辑，生成了一个 32MB 的视频帧
    // 为了简化，我们只是填充一些测试数据
    for (size_t i = 0; i < FRAME_SIZE; ++i) {
        ((char *)shm_ptr)[i] = (char)i % 256;
    }

    // 解除映射
    if (munmap(shm_ptr, FRAME_SIZE) == -1) {
        perror("munmap");
    }

    // 关闭文件
    close(shm_fd);

    return 0;
}

*/