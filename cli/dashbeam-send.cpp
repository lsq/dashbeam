// dashbeam-send: 通过 dashbeam_ffi.dll 分享文件/文件夹的命令行工具
// 用法: dashbeam-send <文件或目录> [更多路径...]
// 使用公共 iroh relay/discovery（默认），打印 ticket 供接收端使用。

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#  include <windows.h>
#endif

#include "../engine/ffi/include/dashbeam.h"

namespace fs = std::filesystem;

static std::atomic<bool> g_ready{false};
static std::atomic<bool> g_failed{false};
static std::atomic<bool> g_quit{false};
static std::atomic<int64_t> g_handle{-1};
static int g_connected_peers = 0;

static void on_event(const char *event, const char *payload, void *user_data) {
    (void)user_data;
    const char *p = payload ? payload : "";

    if (std::strcmp(event, "share-ready") == 0) {
        // payload: {"handle","ticket","hash","size","entry_type"}
        std::string json(p);
        // 简单提取 ticket 字段（ticket 内不含引号/逗号）
        auto pos = json.find("\"ticket\":\"");
        if (pos != std::string::npos) {
            pos += 10;
            auto end = json.find('"', pos);
            std::string ticket = json.substr(pos, end - pos);
            std::printf("\n=== 分享已就绪 ===\n");
            std::printf("ticket: %s\n", ticket.c_str());
            std::printf("(完整回执: %s)\n\n", p);
            std::printf("等待接收端连接... Ctrl+C 停止分享\n");
        } else {
            std::printf("[share-ready] %s\n", p);
        }
        g_ready = true;
    } else if (std::strcmp(event, "share-error") == 0) {
        std::fprintf(stderr, "[share-error] %s\n", p);
        g_failed = true;
    } else if (std::strcmp(event, "share-peer-connected") == 0) {
        ++g_connected_peers;
        std::printf("[peer] 新对端连接 (累计 %d): %s\n", g_connected_peers, p);
    } else if (std::strcmp(event, "transfer-started") == 0) {
        std::printf("[transfer] 开始传输\n");
    } else if (std::strcmp(event, "transfer-progress") == 0) {
        // payload: "bytes:total:speed*1000"
        uint64_t bytes = 0, total = 0, speedx1000 = 0;
        if (std::sscanf(p, "%llu:%llu:%llu",
                        (unsigned long long *)&bytes,
                        (unsigned long long *)&total,
                        (unsigned long long *)&speedx1000) == 3) {
            double pct = total ? 100.0 * bytes / total : 0.0;
            float speedBps = speedx1000 / 1000.0;
            float mbps = speedBps / (1024.0 * 1024.0);
            float kbps = speedBps / 1024.0;
            std::printf("\r[transfer] %llu/%llu bytes (%.1f%%)  %.2f MB/s   ",
                        (unsigned long long)bytes, (unsigned long long)total,
                        pct,  mbps > 1.0 ? mbps : kbps);
            std::fflush(stdout);
        }
    } else if (std::strcmp(event, "transfer-completed") == 0) {
        std::printf("\n[transfer] 完成: %s\n", p);
    } else if (std::strcmp(event, "transfer-failed") == 0) {
        std::printf("\n[transfer] 失败: %s\n", p);
    }
    // "active-connection-count" 等事件太吵，不打印
}

#ifdef _WIN32
static BOOL WINAPI console_ctrl(DWORD type) {
    if (type == CTRL_C_EVENT || type == CTRL_CLOSE_EVENT) {
        g_quit = true;
        return TRUE;
    }
    return FALSE;
}
#else
static void sigint_handler(int) { g_quit = true; }
#endif

int main(int argc, char **argv) {
#ifdef _WIN32
    SetConsoleOutputCP(CP_UTF8); // 保证 ticket/中文输出不乱码
    SetConsoleCtrlHandler(console_ctrl, TRUE);
#else
    std::signal(SIGINT, sigint_handler);
#endif

    if (argc < 2) {
        std::fprintf(stderr,
            "用法: %s <文件或目录> [更多路径...]\n"
            "支持同时传入多个文件和文件夹，将打包为一次分享并输出 ticket。\n",
            argv[0]);
        return 2;
    }

    // 1. 校验并收集路径（文件或目录均可，与 GUI 的 start_share_items 一致）
    std::vector<std::string> storage;
    std::vector<const char *> paths;
    for (int i = 1; i < argc; ++i) {
        std::error_code ec;
        fs::path p = fs::absolute(argv[i], ec);
        if (ec || !fs::exists(p, ec)) {
            std::fprintf(stderr, "路径不存在: %s\n", argv[i]);
            return 2;
        }
        storage.push_back(p.string());
        paths.push_back(storage.back().c_str());
        std::printf("添加: %s\n", storage.back().c_str());
    }

    // 2. 启动分享。options_json=nullptr -> 公共 iroh relay + n0 discovery + Id ticket
    const char *opts = R"({"ticket_type":"RelayAndAddresses"})";
    int64_t h = dashbeam_send_start(paths.data(), paths.size(),
                                    opts, on_event, nullptr);
    if (h < 0) {
        const char *err = dashbeam_last_error();
        std::fprintf(stderr, "启动分享失败: %s\n", err ? err : "unknown");
        return 1;
    }
    g_handle = h;
    std::printf("正在上线（公共 iroh relay）...\n");

    // 3. 等待 share-ready / share-error（首次建 relay 连接可能需要十几秒）
    for (int i = 0; i < 600 && !g_ready && !g_failed && !g_quit; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (g_failed) { dashbeam_send_stop(h); return 1; }
    if (!g_ready) {
        std::fprintf(stderr, "%s\n", g_quit ? "已取消" : "等待 share-ready 超时");
        dashbeam_send_stop(h);
        return 1;
    }

    // 4. 保持分享会话存活，直到 Ctrl+C
    while (!g_quit)
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::printf("\n正在停止分享...\n");
    if (dashbeam_send_stop(h) != 0) {
        const char *err = dashbeam_last_error();
        std::fprintf(stderr, "停止失败: %s\n", err ? err : "unknown");
        return 1;
    }
    std::printf("已停止。\n");
    return 0;
}
