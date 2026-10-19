use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::net::{SocketAddrV4, SocketAddrV6};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use protocol::{
    AddrInfoOptions, AppHandle, DiscoveryModeOption, EventEmitter, FileMetadata,
    RelayModeOption, SendOptions,
};
use native::{start_share_items, SendResult, ShareItem};

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("dashbeam tokio runtime")
    })
}

/// Live share sessions keyed by handle. Dropping a SendResult shuts the router
/// down and auto-cleans the blobs dir, so removal == cancel.
fn sessions() -> &'static Mutex<HashMap<i64, SendResult>> {
    static S: OnceLock<Mutex<HashMap<i64, SendResult>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashMap::new()))
}

static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);

fn last_error_slot() -> &'static Mutex<Option<CString>> {
    static E: OnceLock<Mutex<Option<CString>>> = OnceLock::new();
    E.get_or_init(|| Mutex::new(None))
}

fn set_last_error(msg: &str) {
    if let Ok(mut slot) = last_error_slot().lock() {
        *slot = CString::new(msg).ok();
    }
}

// ---------------------------------------------------------------------------
// Options (JSON) -> protocol::SendOptions
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct FfiSendOptions {
    /// "default" | "disabled" | "custom"
    relay_mode: String,
    /// relay_mode = "custom" 时的 relay URL 列表
    relay_urls: Vec<String>,
    relay_auth_token: Option<String>,
    /// "default" | "custom"
    discovery_mode: String,
    pkarr_relay_url: Option<String>,
    dns_origin: Option<String>,
    /// "Id" | "RelayAndAddresses" | "Relay" | "Addresses"
    ticket_type: String,
    /// e.g. "0.0.0.0:0"
    ipv4_addr: Option<String>,
    ipv6_addr: Option<String>,
    /// 可选文件元数据（直接透传 protocol::FileMetadata 的 serde 形状）
    metadata: Option<FileMetadata>,

    // ---- NEW: 透明显示名（透明加密场景）----------------------------------
    /// 与 paths 平行的显示名数组：display_names[i] 是 paths[i] 在接收端
    /// 看到的根条目名。数组可短于 paths，缺失/null 项用磁盘文件名。
    /// 例: paths=["/d/x.iso.pdf"], display_names=["x.iso"]
    display_names: Vec<Option<String>>,
    /// 对所有条目的文件名剥离的后缀（大小写不敏感），如 ".pdf"。
    /// 对文件条目剥根名；对目录条目递归剥其中每个文件的名字。
    /// 与 display_names 同时存在时，display_names 优先（直接覆盖根名）。
    strip_suffix: Option<String>,
}

fn map_options(o: &FfiSendOptions) -> anyhow::Result<SendOptions> {
    let relay_mode = match o.relay_mode.as_str() {
        "" | "default" => RelayModeOption::Default,
        "disabled" => RelayModeOption::Disabled,
        "custom" => {
            let urls = o
                .relay_urls
                .iter()
                .map(|s| iroh::RelayUrl::from_str(s).map_err(|e| anyhow::anyhow!("bad relay url {s}: {e}")))
                .collect::<anyhow::Result<Vec<_>>>()?;
            RelayModeOption::Custom { urls, auth_token: o.relay_auth_token.clone() }
        }
        other => anyhow::bail!("unknown relay_mode: {other}"),
    };

    let discovery_mode = match o.discovery_mode.as_str() {
        "" | "default" => DiscoveryModeOption::Default,
        "custom" => {
            let url = o
                .pkarr_relay_url
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("custom discovery requires pkarr_relay_url"))
                .and_then(|s| url::Url::parse(s).map_err(|e| anyhow::anyhow!("bad pkarr url: {e}")))?;
            DiscoveryModeOption::Custom { pkarr_relay_url: url, dns_origin: o.dns_origin.clone(), }
        }
        other => anyhow::bail!("unknown discovery_mode: {other}"),
    };

    let ticket_type = if o.ticket_type.is_empty() {
        AddrInfoOptions::Id
    } else {
        AddrInfoOptions::from_str(&o.ticket_type)
            .map_err(|e| anyhow::anyhow!("bad ticket_type: {e}"))?
    };

    let magic_ipv4_addr = o
        .ipv4_addr
        .as_deref()
        .map(SocketAddrV4::from_str)
        .transpose()
        .map_err(|e| anyhow::anyhow!("bad ipv4_addr: {e}"))?;
    let magic_ipv6_addr = o
        .ipv6_addr
        .as_deref()
        .map(SocketAddrV6::from_str)
        .transpose()
        .map_err(|e| anyhow::anyhow!("bad ipv6_addr: {e}"))?;

    Ok(
        SendOptions { relay_mode, discovery_mode, ticket_type, magic_ipv4_addr, magic_ipv6_addr },
    )
}

/// NEW: 把 FFI 路径数组 + 显示名/剥后缀选项组装成 Vec<ShareItem>。
fn build_items(path_list: Vec<PathBuf>, o: &FfiSendOptions) -> Vec<ShareItem> {
    path_list
        .into_iter()
        .enumerate()
        .map(|(i, path)| ShareItem {
            path,
            display_name: o.display_names.get(i).cloned().flatten(),
            strip_suffix: o.strip_suffix.clone(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// EventEmitter -> C callback
// ---------------------------------------------------------------------------

/// event/payload 字符串仅在回调执行期间有效，C++ 侧需要复制。
pub type DashbeamEventCb = extern "C" fn(*const c_char, *const c_char, *mut c_void);

struct CbEmitter {
    cb: DashbeamEventCb,
    user_data: usize, // 原始指针按值传递
}

unsafe impl Send for CbEmitter {}
unsafe impl Sync for CbEmitter {}

impl CbEmitter {
    fn fire(&self, name: &str, payload: Option<&str>) -> Result<(), String> {
        let name = CString::new(name).map_err(|e| e.to_string())?;
        let payload = match payload {
            Some(p) => CString::new(p).map_err(|e| e.to_string())?,
            None => CString::new("").map_err(|e| e.to_string())?,
        };
        (self.cb)(name.as_ptr(), payload.as_ptr(), self.user_data as *mut c_void);
        Ok(())
    }
}

impl EventEmitter for CbEmitter {
    fn emit_event(&self, event_name: &str) -> Result<(), String> {
        self.fire(event_name, None)
    }
    fn emit_event_with_payload(&self, event_name: &str, payload: &str) -> Result<(), String> {
        self.fire(event_name, Some(payload))
    }
}

// ---------------------------------------------------------------------------
// C ABI
// ---------------------------------------------------------------------------

/// 启动分享。立即返回句柄（>0）；分享就绪后以 "share-ready" 事件回调
/// {"ticket","hash","size","entry_type"}，失败回调 "share-error"（payload 为错误文本）。
/// options_json 可为 NULL（全默认）。返回 -1 表示参数错误，错误文本见 dashbeam_last_error。
#[no_mangle]
pub extern "C" fn dashbeam_send_start(
    paths: *const *const c_char,
    path_count: usize,
    options_json: *const c_char,
    event_cb: Option<DashbeamEventCb>,
    user_data: *mut c_void,
) -> i64 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if paths.is_null() || path_count == 0 {
            return Err(anyhow::anyhow!("paths is null or empty"));
        }
        let mut path_list = Vec::with_capacity(path_count);
        for i in 0..path_count {
            let p = unsafe { *paths.add(i) };
            if p.is_null() {
                return Err(anyhow::anyhow!("paths[{i}] is null"));
            }
            let s = unsafe { CStr::from_ptr(p) }
                .to_str()
                .map_err(|e| anyhow::anyhow!("paths[{i}] not utf-8: {e}"))?;
            path_list.push(PathBuf::from(s));
        }

        let ffi_opts: FfiSendOptions = if options_json.is_null() {
            FfiSendOptions::default()
        } else {
            let json = unsafe { CStr::from_ptr(options_json) }
                .to_str()
                .map_err(|e| anyhow::anyhow!("options not utf-8: {e}"))?;
            serde_json::from_str(json)
                .map_err(|e| anyhow::anyhow!("bad options json: {e}"))?
        };
        let options = map_options(&ffi_opts)?;
        let metadata = ffi_opts.metadata.clone();
        let items = build_items(path_list, &ffi_opts);            // NEW

        let handle_id = NEXT_HANDLE.fetch_add(1, Ordering::SeqCst);

        // AppHandle: 有回调就包一层 CbEmitter，没有则为 None
        let app_handle: AppHandle = event_cb.map(|cb| {
            Arc::new(CbEmitter { cb, user_data: user_data as usize }) as Arc<dyn EventEmitter>
        });

        let cb = event_cb;
        let ud = user_data as usize;

        runtime().spawn(async move {
            let emit = |name: &str, payload: &str| {
                if let Some(cb) = cb {
                    if let (Ok(n), Ok(p)) = (CString::new(name), CString::new(payload)) {
                        cb(n.as_ptr(), p.as_ptr(), ud as *mut c_void);
                    }
                }
            };

            match start_share_items(items, options, &app_handle, metadata).await {
                Ok(result) => {
                    let ready = serde_json::json!({
                        "handle": handle_id,
                        "ticket": result.ticket,
                        "hash": result.hash,
                        "size": result.size,
                        "entry_type": result.entry_type,
                    });
                    emit("share-ready", &ready.to_string());
                    sessions()
                        .lock()
                        .expect("sessions poisoned")
                        .insert(handle_id, result);
                }
                Err(e) => {
                    emit("share-error", &format!("{e:#}"));
                }
            }
        });

        Ok(handle_id)
    }));

    match result {
        Ok(Ok(h)) => h,
        Ok(Err(e)) => {
            set_last_error(&format!("{e:#}"));
            -1
        }
        Err(_) => {
            set_last_error("panic in dashbeam_send_start");
            -1
        }
    }
}

/// 取消分享并释放句柄。成功返回 0，句柄不存在返回 -1。
/// runtime().block_on：因为 extern "C" 函数本身不是 async，
/// 这里借用已有的全局 tokio runtime 同步跑完这段 async
/// 收尾逻辑；调用方线程会阻塞最多 ~2 秒，C++ 侧
/// 如果不想阻塞主线程，应在单独线程调用 dashbeam_send_stop。
/// 停止并释放分享会话，可选地取出停止前最后一次的已完成对端数。
///
/// out_completed_peers 可为 NULL（不需要该信息）；非 NULL 时，无论成功与否
/// 都会写入 -1（句柄无效/未知）或实际计数（句柄有效，在 router 关闭前读取，
/// 与"停止"原子化，避免 completed_peers() 和 stop() 两次调用之间的竞态）。
/// 返回 0 成功，-1 句柄无效。
///
// #[no_mangle]
// pub extern "C" fn dashbeam_send_stop(handle: i64, out_completed_peers: *mut i64) -> i32 {
//     // 在同一次 lock 里取出 session 并顺带读 completed_peers，
//     // 避免 "先查 completed_peers 再 stop" 两次调用之间对端又完成一次的竞态。
//     let session = match catch_unwind(AssertUnwindSafe(|| {
//         let mut guard = sessions().lock().expect("sessions poisoned");
//         match guard.remove(&handle) {
//             Some(s) => {
//                 let count = s.completed_peers() as i64;
//                 Some((s, count))
//             }
//             None => None,
//         }
//     })) {
//         Ok(Some((s, count))) => {
//             if !out_completed_peers.is_null() {
//                 unsafe { *out_completed_peers = count };
//             }
//             s
//         }
//         Ok(None) => {
//             if !out_completed_peers.is_null() {
//                 unsafe { *out_completed_peers = -1 };
//             }
//             set_last_error("unknown handle");
//             return -1;
//         }
//         Err(_) => {
//             if !out_completed_peers.is_null() {
//                 unsafe { *out_completed_peers = -1 };
//             }
//             set_last_error("panic in dashbeam_send_stop");
//             return -1;
//         }
//     };
//
//     // 镜像 ShareHandle::stop()：2s 超时的 router.shutdown() + endpoint.close()。
//     let outcome = catch_unwind(AssertUnwindSafe(|| {
//         runtime().block_on(async {
//             use std::time::Duration;
//             match tokio::time::timeout(Duration::from_secs(2), session.router.shutdown()).await {
//                 Ok(Ok(())) => {}
//                 Ok(Err(e)) => tracing::warn!("router shutdown error: {e}"),
//                 Err(_) => tracing::warn!("router shutdown timeout after 2s"),
//             }
//             session.router.endpoint().close().await;
//             // session 在这里 drop：AutoCleanupDir 清理 blobs 目录
//         })
//     }));
//
//     match outcome {
//         Ok(()) => 0,
//         Err(_) => {
//             set_last_error("panic during router shutdown");
//             -1
//         }
//     }
// }

/// 方案二：接口不变
/// 取消分享并释放句柄。成功返回 0，句柄不存在返回 -1。
/// ABI 不变：和之前一样只接收 handle，不做 completed_peers 的原子读取——
/// 如果 C++ 侧需要停止前的对端数，请在停止前用 dashbeam_send_completed_peers
/// 做定时轮询自行缓存，不再由 FFI 层保证原子性。
/// c++侧后台线程，每隔一个周期（例如 500ms）调用 dashbeam_send_completed_peers，结果存进一个原子变量
#[no_mangle]
pub extern "C" fn dashbeam_send_stop(handle: i64) -> i32 {
    let session = match catch_unwind(AssertUnwindSafe(|| {
        sessions().lock().expect("sessions poisoned").remove(&handle)
    })) {
        Ok(Some(s)) => s,
        Ok(None) => {
            set_last_error("unknown handle");
            return -1;
        }
        Err(_) => {
            set_last_error("panic in dashbeam_send_stop");
            return -1;
        }
    };

    // 镜像 ShareHandle::stop()：2s 超时的 router.shutdown() + endpoint.close()。
    // 2s 阻塞按约定可以忽略，不做额外的异步化处理。
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        runtime().block_on(async {
            use std::time::Duration;
            match tokio::time::timeout(Duration::from_secs(2), session.router.shutdown()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!("router shutdown error: {e}"),
                Err(_) => tracing::warn!("router shutdown timeout after 2s"),
            }
            session.router.endpoint().close().await;
            // session 在这里 drop：AutoCleanupDir 清理 blobs 目录
        })
    }));

    match outcome {
        Ok(()) => 0,
        Err(_) => {
            set_last_error("panic during router shutdown");
            -1
        }
    }
}

/// 已完成完整传输的对端数（share 会话内累计）。
#[no_mangle]
pub extern "C" fn dashbeam_send_completed_peers(handle: i64) -> i64 {
    catch_unwind(AssertUnwindSafe(|| {
        sessions()
            .lock()
            .expect("sessions poisoned")
            .get(&handle)
            .map(|r| r.completed_peers() as i64)
            .unwrap_or(-1)
    }))
    .unwrap_or(-1)
}

/// 返回最后一次错误的线程不安全字符串（仅在下次 FFI 调用前有效）。可为 NULL。
#[no_mangle]
pub extern "C" fn dashbeam_last_error() -> *const c_char {
    last_error_slot()
        .lock()
        .ok()
        .and_then(|s| s.as_ref().map(|c| c.as_ptr()))
        .unwrap_or(std::ptr::null())
}
