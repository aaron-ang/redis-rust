use std::fs;

use resp::Value;
use tempfile::tempdir;

use redis_rust::AofWriter;

#[test]
fn setup_creates_dir_file_and_manifest_when_missing() {
    let tmp = tempdir().unwrap();
    let writer = AofWriter::setup(tmp.path(), "appendonlydir", "appendonly.aof", false).unwrap();
    drop(writer);

    let aof_dir = tmp.path().join("appendonlydir");
    assert!(aof_dir.is_dir());

    let incr = aof_dir.join("appendonly.aof.1.incr.aof");
    assert!(incr.is_file());
    assert_eq!(fs::read(&incr).unwrap(), b"");

    let manifest = aof_dir.join("appendonly.aof.manifest");
    assert_eq!(
        fs::read_to_string(manifest).unwrap(),
        "file appendonly.aof.1.incr.aof seq 1 type i\n"
    );
}

#[test]
fn setup_reuses_existing_manifest_incr_filename() {
    let tmp = tempdir().unwrap();
    let aof_dir = tmp.path().join("appendonlydir");
    fs::create_dir_all(&aof_dir).unwrap();

    let random_incr = "xyz123.1.incr.aof";
    fs::write(aof_dir.join(random_incr), b"").unwrap();
    fs::write(
        aof_dir.join("appendonly.aof.manifest"),
        format!("file {random_incr} seq 1 type i\n"),
    )
    .unwrap();

    let writer = AofWriter::setup(tmp.path(), "appendonlydir", "appendonly.aof", true).unwrap();
    writer
        .append(&Value::Array(vec![
            Value::Bulk("SET".into()),
            Value::Bulk("k".into()),
            Value::Bulk("v".into()),
        ]))
        .unwrap();
    drop(writer);

    let written = fs::read(aof_dir.join(random_incr)).unwrap();
    assert_eq!(
        written,
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".to_vec()
    );
    // The default-named file should NOT have been created or written.
    assert!(!aof_dir.join("appendonly.aof.1.incr.aof").exists());
}

#[test]
fn setup_errors_on_malformed_manifest() {
    let tmp = tempdir().unwrap();
    let aof_dir = tmp.path().join("appendonlydir");
    fs::create_dir_all(&aof_dir).unwrap();
    fs::write(
        aof_dir.join("appendonly.aof.manifest"),
        "not a valid manifest line\n",
    )
    .unwrap();

    assert!(AofWriter::setup(tmp.path(), "appendonlydir", "appendonly.aof", false).is_err());
}

#[test]
fn append_concatenates_multiple_commands() {
    let tmp = tempdir().unwrap();
    let writer = AofWriter::setup(tmp.path(), "appendonlydir", "appendonly.aof", false).unwrap();

    writer
        .append(&Value::Array(vec![
            Value::Bulk("SET".into()),
            Value::Bulk("a".into()),
            Value::Bulk("1".into()),
        ]))
        .unwrap();
    writer
        .append(&Value::Array(vec![
            Value::Bulk("INCR".into()),
            Value::Bulk("a".into()),
        ]))
        .unwrap();
    drop(writer);

    let contents = fs::read(
        tmp.path()
            .join("appendonlydir")
            .join("appendonly.aof.1.incr.aof"),
    )
    .unwrap();
    assert_eq!(
        contents,
        b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$4\r\nINCR\r\n$1\r\na\r\n".to_vec()
    );
}
