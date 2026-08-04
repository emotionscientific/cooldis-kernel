use super::*;

#[test]
fn parses_lenient_heredoc_and_move_hunk() {
    let patch = r#"<<'EOF'
*** Begin Patch
*** Update File: a.txt
*** Move to: b.txt
@@
-old
+new
*** End Patch
EOF"#;
    let hunks = parse_patch(patch).unwrap();
    assert_eq!(hunks.len(), 1);
    assert!(matches!(
        &hunks[0],
        Hunk::UpdateFile {
            path,
            move_path: Some(move_path),
            ..
        } if path == Path::new("a.txt") && move_path == Path::new("b.txt")
    ));
}

#[test]
fn computes_eof_replacement_from_codex_patch_chunks() {
    let chunks = vec![UpdateFileChunk {
        change_context: None,
        old_lines: vec!["tail".to_string()],
        new_lines: vec!["tail".to_string(), "after".to_string()],
        is_end_of_file: true,
    }];
    let next =
        derive_new_contents_from_chunks(Path::new("/file.txt"), "head\ntail\n", &chunks).unwrap();
    assert_eq!(next, "head\ntail\nafter\n");
}
