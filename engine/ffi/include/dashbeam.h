#ifndef DASHBEAM_H
#define DASHBEAM_H

#include <stdint.h>
#include <stddef.h>

#ifdef _WIN32
#  ifdef DASHBEAM_BUILD
#    define DASHBEAM_API __declspec(dllexport)
#  else
#    define DASHBEAM_API __declspec(dllimport)
#  endif
#else
#  define DASHBEAM_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*
 * 进度/生命周期事件回调。
 *   event:   "share-ready" | "share-error" | "transfer-started" |
 *            "transfer-progress" | "transfer-completed" | "transfer-failed" |
 *            "active-connection-count" | "share-peer-connected"
 *   payload: share-ready -> {"handle","ticket","hash","size","entry_type"}
 *            share-error -> 错误文本
 *            transfer-progress -> "bytes:total:speed*1000"（冒号分隔）
 *            transfer-completed -> {"durationMs","bytes","totalBytes"}
 *            active-connection-count / share-peer-connected -> 文本/JSON
 * 注意：回调发生在 tokio worker 线程上；event/payload 仅在回调期间有效，
 * 需要保留请自行复制；如需更新 UI 请 marshal 到 UI 线程。
 */
typedef void (*dashbeam_event_cb)(const char *event,
                                  const char *payload,
                                  void *user_data);

/*
 * options_json 可为 NULL（等价于 "{}"）。字段全部可选：
 * {
 *   "relay_mode": "default"|"disabled"|"custom",
 *   "relay_urls": ["https://..."],          // custom 时必填
 *   "relay_auth_token": "...",              // 可选
 *   "discovery_mode": "default"|"custom",
 *   "pkarr_relay_url": "https://...",       // custom discovery 必填
 *   "dns_origin": "example.com",            // 可选
 *   "ticket_type": "Id"|"RelayAndAddresses"|"Relay"|"Addresses",
 *   "ipv4_addr": "0.0.0.0:0",
 *   "ipv6_addr": "[::]:0",
 *   "metadata": {"file_name":"...","item_count":1,"size":123,...}
 *   "display_names": ["x.iso", null, ...],  // 与 paths 平行，可选
 *   "strip_suffix": ".pdf",                 // 递归剥离文件后缀，可选
 * }
 *
 * 返回 >0 的句柄；返回 -1 时调用 dashbeam_last_error() 取错误。
 */
DASHBEAM_API int64_t dashbeam_send_start(const char *const *paths,
                                         size_t path_count,
                                         const char *options_json,
                                         dashbeam_event_cb event_cb,
                                         void *user_data);

/* 停止并释放分享会话。返回 0 成功，-1 句柄无效。 */
DASHBEAM_API int32_t dashbeam_send_stop(int64_t handle);

/* 停止并释放分享会话。
 * out_completed_peers 可传 NULL；非 NULL 时会写入停止前（与关闭过程原子）
 * 读取到的已完成对端数，句柄无效时写 -1。
 * 返回 0 成功，-1 句柄无效。
 */
// DASHBEAM_API int32_t dashbeam_send_stop(int64_t handle, int64_t *out_completed_peers);

/* 已完成传输的对端数；句柄无效返回 -1。 */
DASHBEAM_API int64_t dashbeam_send_completed_peers(int64_t handle);

/* 最近一次的 FFI 错误文本；无错误返回 NULL。指针内部持有，不要 free。 */
DASHBEAM_API const char *dashbeam_last_error(void);

#ifdef __cplusplus
}
#endif
#endif /* DASHBEAM_H */
