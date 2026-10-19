// dashbeam-send: 通过 dashbeam_ffi.dll 分享文件/文件夹的命令行工具
// 用法: dashbeam-send [--strip-suffix <后缀>] <文件或目录> [更多路径...]
//
// --strip-suffix .pdf: 接收端看到的文件名会剥掉 ".pdf" 后缀（大小写不敏感），
//   磁盘上仍以原名读取 —— 用于透明加密环境（磁盘名 x.iso.pdf -> 对端 x.iso）。
//   对目录条目递归生效。
//
// 默认使用公共 iroh relay/discovery + RelayAndAddresses ticket。

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
static int g_connected_peers = 0;

static void on_event(const char *event, const char *payload, void *user_data) {
    (void)user_data;
    const char *p = payload ? payload : "";

    if (std::strcmp(event, "share-ready") == 0) {
        // payload: {"handle","ticket","hash","size","entry_type"}
        std::string json(p);
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
    // "active-connection-count" 太吵，不打印
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

// ---- 参数解析 --------------------------------------------------------------

struct CliArgs {
    std::string strip_suffix;          // 为空 = 不剥离
    std::vector<std::string> paths;    // 位置参数：文件/目录
};

static void usage(const char *prog) {
    std::fprintf(stderr,
        "用法: %s [--strip-suffix <后缀>] <文件或目录> [更多路径...]\n"
        "\n"
        "选项:\n"
        "  --strip-suffix <后缀>   接收端文件名剥离该后缀（如 .pdf），磁盘名不变\n"
        "  -h, --help              显示本帮助\n",
        prog);
}

// 返回 false = 解析失败或请求 help（已打印 usage）
static bool parse_args(int argc, char **argv, CliArgs &out, bool &help) {
    help = false;
    for (int i = 1; i < argc; ++i) {
        const char *a = argv[i];
        if (std::strcmp(a, "-h") == 0 || std::strcmp(a, "--help") == 0) {
            help = true;
            return false;
        }
        if (std::strcmp(a, "--strip-suffix") == 0) {
            if (++i >= argc) {
                std::fprintf(stderr, "--strip-suffix 缺少参数\n");
                return false;
            }
            out.strip_suffix = argv[i];
            // 允许 "pdf" 或 ".pdf" 两种写法，统一补点
            if (!out.strip_suffix.empty() && out.strip_suffix[0] != '.')
                out.strip_suffix = "." + out.strip_suffix;
            continue;
        }
        if (std::strncmp(a, "--strip-suffix=", 15) == 0) {
            out.strip_suffix = a + 15;
            if (!out.strip_suffix.empty() && out.strip_suffix[0] != '.')
                out.strip_suffix = "." + out.strip_suffix;
            continue;
        }
        if (a[0] == '-' && a[1] != '\0') {
            std::fprintf(stderr, "未知选项: %s\n", a);
            return false;
        }
        out.paths.push_back(a);
    }
    return true;
}

// 最小 JSON 字符串转义（后缀里出现引号/反斜杠才需要）
static std::string json_escape(const std::string &s) {
    std::string r;
    for (char c : s) {
        switch (c) {
            case '"':  r += "\\\""; break;
            case '\\': r += "\\\\"; break;
            default:   r += c;
        }
    }
    return r;
}

// ---- main ------------------------------------------------------------------

int main(int argc, char **argv) {
#ifdef _WIN32
    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCtrlHandler(console_ctrl, TRUE);
#else
    std::signal(SIGINT, sigint_handler);
#endif

    CliArgs args;
    bool help = false;
    if (!parse_args(argc, argv, args, help) || args.paths.empty()) {
        if (!help)
            std::fprintf(stderr, "%s\n",
                         help ? "" : "错误: 参数无效或没有提供任何路径\n");
        usage(argv[0]);
        return 2;
    }

    // 1. 校验并收集路径（文件或目录均可）
    std::vector<std::string> storage;
    std::vector<const char *> paths;
    for (const auto &a : args.paths) {
        std::error_code ec;
        fs::path p = fs::absolute(a, ec);
        if (ec || !fs::exists(p, ec)) {
            std::fprintf(stderr, "路径不存在: %s\n", a.c_str());
            return 2;
        }
        storage.push_back(p.string());
        paths.push_back(storage.back().c_str());
        std::printf("添加: %s\n", storage.back().c_str());
    }

    // 2. 构造 options JSON：RelayAndAddresses ticket（避免 Id ticket 的
    //    DNS 解析时序问题）+ 可选 strip_suffix
    std::string opts = R"({"ticket_type":"RelayAndAddresses")";
    if (!args.strip_suffix.empty()) {
        opts += ",\"strip_suffix\":\"" + json_escape(args.strip_suffix) + "\"";
        std::printf("strip-suffix: %s（接收端文件名将剥掉该后缀）\n",
                    args.strip_suffix.c_str());
    }
    opts += "}";

    // 3. 启动分享
    int64_t h = dashbeam_send_start(paths.data(), paths.size(),
                                    opts.c_str(), on_event, nullptr);
    if (h < 0) {
        const char *err = dashbeam_last_error();
        std::fprintf(stderr, "启动分享失败: %s\n", err ? err : "unknown");
        return 1;
    }
    std::printf("正在上线（公共 iroh relay）...\n");

    // 4. 等待 share-ready / share-error
    for (int i = 0; i < 600 && !g_ready && !g_failed && !g_quit; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (g_failed) { dashbeam_send_stop(h); return 1; }
    if (!g_ready) {
        std::fprintf(stderr, "%s\n", g_quit ? "已取消" : "等待 share-ready 超时");
        dashbeam_send_stop(h);
        return 1;
    }

    // 5. 保持分享会话存活，直到 Ctrl+C
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
