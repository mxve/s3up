use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use std::{env, path::Path, process};

fn require_env(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| {
        eprintln!("error: {key} not set");
        process::exit(1);
    })
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::from_path(".env");

    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: s3up <file> [file2 ...]");
        process::exit(2);
    }

    let bucket = require_env("S3_BUCKET");
    let endpoint = require_env("AWS_ENDPOINT_URL");
    let region = env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "auto".to_string());

    let access_key = env::var("AWS_ACCESS_KEY_ID").ok();
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY").ok();

    let config = {
        let mut builder = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(aws_config::Region::new(region));

        if let (Some(ak), Some(sk)) = (access_key, secret_key) {
            let creds = Credentials::new(ak, sk, None, None, "env");
            builder = builder.credentials_provider(creds);
        }

        builder.load().await
    };

    let client = aws_sdk_s3::Client::new(&config);
    let mut had_error = false;

    for file_path in &args {
        let path = Path::new(file_path);

        if !path.exists() {
            eprintln!("error: file not found: {file_path}");
            had_error = true;
            continue;
        }

        let key = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| file_path.clone());

        let body = match ByteStream::from_path(path).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("error reading {file_path}: {e}");
                had_error = true;
                continue;
            }
        };

        match client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(body)
            .send()
            .await
        {
            Ok(_) => println!("uploaded: {file_path} -> {bucket}/{key}"),
            Err(e) => {
                eprintln!("error uploading {file_path}: {e}");
                had_error = true;
            }
        }
    }

    process::exit(if had_error { 1 } else { 0 });
}
