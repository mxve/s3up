use crate::parse_args;

macro_rules! parse {
    ($($arg:expr),*) => {
        parse_args(&[$($arg.to_string()),*])
    };
}

#[test]
fn empty() {
    let result = parse!();
    assert!(result.unwrap_err().contains("usage:"));
}

#[test]
fn normal() {
    let result = parse!("file1.txt", "dir/file2.txt").unwrap();
    assert_eq!(result.uploads.len(), 2);
    assert_eq!(result.uploads[0], ("file1.txt".into(), "file1.txt".into()));
    assert_eq!(
        result.uploads[1],
        ("dir/file2.txt".into(), "file2.txt".into())
    );
    assert_eq!(result.concurrency, 1);
}

#[test]
fn normal_paths_and_spaces() {
    let result = parse!("/absolute/path/file.txt", "relative/path/with spaces.png").unwrap();
    assert_eq!(result.uploads.len(), 2);
    assert_eq!(
        result.uploads[0],
        ("/absolute/path/file.txt".into(), "file.txt".into())
    );
    assert_eq!(
        result.uploads[1],
        (
            "relative/path/with spaces.png".into(),
            "with spaces.png".into()
        )
    );
}

#[test]
fn rename_valid() {
    let result = parse!(
        "--rename",
        "local1.txt",
        "s3_key1.txt",
        "local2.png",
        "images/pic.png"
    )
    .unwrap();
    assert_eq!(result.uploads.len(), 2);
    assert_eq!(
        result.uploads[0],
        ("local1.txt".into(), "s3_key1.txt".into())
    );
    assert_eq!(
        result.uploads[1],
        ("local2.png".into(), "images/pic.png".into())
    );
}

#[test]
fn rename_invalid_odd() {
    let result = parse!("--rename", "local1.txt", "s3_key1.txt", "local2.png");
    assert!(result.unwrap_err().contains("--rename requires pairs"));
}

#[test]
fn rename_no_files() {
    let result = parse!("--rename");
    assert!(result.is_err());
}

#[test]
fn trailing_slash_fallback() {
    let result = parse!("my_folder/").unwrap();
    assert_eq!(result.uploads.len(), 1);
    assert_eq!(result.uploads[0], ("my_folder/".into(), "my_folder".into()));
}

#[test]
fn concurrency_zero() {
    let result = parse!("--concurrency", "0", "file.txt");
    assert!(result.unwrap_err().contains("at least 1"));
}

#[test]
fn concurrency_invalid() {
    let result = parse!("--concurrency", "abc", "file.txt");
    assert!(result.unwrap_err().contains("positive integer"));
}

#[test]
fn concurrency_missing_value() {
    let result = parse!("--concurrency");
    assert!(result.unwrap_err().contains("requires a value"));
}

#[test]
fn rename_and_concurrency() {
    let result = parse!("--rename", "--concurrency", "8", "a.txt", "b.txt").unwrap();
    assert_eq!(result.concurrency, 8);
    assert_eq!(result.uploads[0], ("a.txt".into(), "b.txt".into()));
}

#[test]
fn flags_after_files_treated_as_files() {
    let result = parse!("file.txt", "--rename").unwrap();
    assert_eq!(result.uploads.len(), 2);
    assert_eq!(result.uploads[1].0, "--rename");
}
