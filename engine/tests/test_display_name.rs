mod common;

use common::TestFixture;
use engine::{download, start_share_items, ReceiveOptions, SendOptions, ShareItem};
use std::path::PathBuf;

/// Disk name carries the ".pdf" wrapper (transparent-encryption scenario),
/// receiver must see the original name.
#[tokio::test]
async fn e2e_strip_suffix_renames_received_file() {
    let fixture = TestFixture::new();
    let content = b"iso bytes";
    // On disk: payload.iso.pdf — receiver should get payload.iso
    let file = fixture.create_file("payload.iso.pdf", content);
    let recv_dir = fixture.output_dir();

    let share = start_share_items(
        vec![ShareItem {
            path: file,
            display_name: None,
            strip_suffix: Some(".pdf".into()),
        }],
        SendOptions::default(),
        &None,
        None,
    )
    .await
    .expect("start_share_items should succeed");

    let (_cancel_tx, cancel_rx) = common::no_cancel();
    download(
        share.ticket.clone(),
        ReceiveOptions {
            output_dir: Some(recv_dir.clone()),
            ..Default::default()
        },
        None,
        cancel_rx,
    )
    .await
    .expect("download should succeed");

    assert_eq!(
        std::fs::read(recv_dir.join("payload.iso")).unwrap(),
        content
    );
    assert!(!recv_dir.join("payload.iso.pdf").exists());
    drop(share);
}

/// Directory: suffix is stripped recursively from every file inside.
#[tokio::test]
async fn e2e_strip_suffix_renames_files_in_dir() {
    let fixture = TestFixture::new();
    let dir = fixture.create_dir_with_files(
        "folder",
        &[
            ("a.iso.pdf", b"AAA"),
            ("sub/b.iso.pdf", b"BBB"),
            ("plain.txt", b"CCC"),
        ],
    );
    let recv_dir = fixture.output_dir();

    let share = start_share_items(
        vec![ShareItem {
            path: dir,
            display_name: None,
            strip_suffix: Some(".pdf".into()),
        }],
        SendOptions::default(),
        &None,
        None,
    )
    .await
    .expect("start_share_items should succeed");

    let (_cancel_tx, cancel_rx) = common::no_cancel();
    download(
        share.ticket.clone(),
        ReceiveOptions {
            output_dir: Some(recv_dir.clone()),
            ..Default::default()
        },
        None,
        cancel_rx,
    )
    .await
    .expect("download should succeed");

    assert_eq!(
        std::fs::read(recv_dir.join("folder/a.iso")).unwrap(),
        b"AAA"
    );
    assert_eq!(
        std::fs::read(recv_dir.join("folder/sub/b.iso")).unwrap(),
        b"BBB"
    );
    // 不带后缀的文件不受影响
    assert_eq!(
        std::fs::read(recv_dir.join("folder/plain.txt")).unwrap(),
        b"CCC"
    );
    drop(share);
}

/// display_name overrides the root entry name entirely.
#[tokio::test]
async fn e2e_display_name_overrides_root() {
    let fixture = TestFixture::new();
    let file = fixture.create_file("real.iso.pdf", b"data");
    let recv_dir = fixture.output_dir();

    let share = start_share_items(
        vec![ShareItem {
            path: file,
            display_name: Some("nice-name.iso".into()),
            strip_suffix: None,
        }],
        SendOptions::default(),
        &None,
        None,
    )
    .await
    .expect("start_share_items should succeed");

    let (_cancel_tx, cancel_rx) = common::no_cancel();
    download(
        share.ticket.clone(),
        ReceiveOptions {
            output_dir: Some(recv_dir.clone()),
            ..Default::default()
        },
        None,
        cancel_rx,
    )
    .await
    .expect("download should succeed");

    assert_eq!(
        std::fs::read(recv_dir.join("nice-name.iso")).unwrap(),
        b"data"
    );
    drop(share);
}

/// PathBuf -> ShareItem ergonomic conversion keeps plain callers terse.
#[test]
fn share_item_plain_preserves_name() {
    let item = ShareItem::plain(PathBuf::from("x.iso.pdf"));
    assert_eq!(item.root_name(), "x.iso.pdf");
}
