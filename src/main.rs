use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use std::sync::Arc;
use std::{env, path::Path, process::ExitCode};
use tokio::sync::Semaphore;

#[cfg(test)]
mod tests;

fn require_env(key: &str) -> Option<String> {
    env::var(key)
        .map_err(|_| eprintln!("error: {key} not set"))
        .ok()
}

#[derive(Debug)]
pub(crate) struct Args {
    pub concurrency: usize,
    pub uploads: Vec<(String, String)>,
}

pub(crate) fn parse_args(args: &[String]) -> Result<Args, String> {
    let mut rename = false;
    let mut concurrency = 1;
    let mut idx = 0;

    while let Some(arg) = args.get(idx) {
        match arg.as_str() {
            "--rename" => rename = true,
            "--concurrency" => {
                let val = args
                    .get(idx + 1)
                    .ok_or("error: --concurrency requires a value")?;
                concurrency = val.parse::<usize>().map_err(|_| {
                    format!("error: --concurrency value must be a positive integer, got '{val}'")
                })?;
                if concurrency == 0 {
                    return Err("error: --concurrency must be at least 1".into());
                }
                idx += 1;
            }
            _ => break,
        }
        idx += 1;
    }

    let file_args = &args[idx..];
    if file_args.is_empty() {
        return Err("usage: s3up [--rename] [--concurrency <n>] <file> [file2 ...]\n       s3up --rename [--concurrency <n>] <local_file> <s3_key> [<local_file> <s3_key> ...]".into());
    }

    if rename && !file_args.len().is_multiple_of(2) {
        return Err("error: --rename requires pairs of <local_file> <s3_key>".into());
    }

    let uploads = if rename {
        file_args
            .chunks(2)
            .map(|p| (p[0].clone(), p[1].clone()))
            .collect()
    } else {
        file_args
            .iter()
            .map(|f| {
                let key = Path::new(f)
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| f.clone());
                (f.clone(), key)
            })
            .collect()
    };

    Ok(Args {
        concurrency,
        uploads,
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = dotenvy::from_path(".env");

    let raw_args: Vec<String> = env::args().skip(1).collect();
    let args = match parse_args(&raw_args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };

    let Some(bucket) = require_env("S3_BUCKET") else {
        return ExitCode::FAILURE;
    };
    let Some(endpoint) = require_env("AWS_ENDPOINT_URL") else {
        return ExitCode::FAILURE;
    };

    let region = env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "auto".into());
    let access_key = env::var("AWS_ACCESS_KEY_ID").ok();
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY").ok();

    let config = {
        let mut builder = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(aws_config::Region::new(region));
        if let (Some(ak), Some(sk)) = (access_key, secret_key) {
            builder = builder.credentials_provider(Credentials::new(ak, sk, None, None, "env"));
        }
        builder.load().await
    };

    let client = Arc::new(aws_sdk_s3::Client::new(&config));
    let bucket = Arc::new(bucket);
    let sem = Arc::new(Semaphore::new(args.concurrency));

    let mut handles = Vec::with_capacity(args.uploads.len());

    for (file_path, key) in args.uploads {
        let client = client.clone();
        let bucket = bucket.clone();
        let sem = sem.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let path = Path::new(&file_path);

            if !path.exists() {
                eprintln!("error: file not found: {file_path}");
                return false;
            }

            let ctx = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();
            let body = match ByteStream::from_path(path).await {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("error reading {file_path}: {e}");
                    return false;
                }
            };

            match client
                .put_object()
                .bucket(bucket.as_ref())
                .key(&key)
                .content_type(&ctx)
                .body(body)
                .send()
                .await
            {
                Ok(_) => {
                    println!("uploaded: {file_path} -> {bucket}/{key} ({ctx})");
                    true
                }
                Err(e) => {
                    eprintln!("error uploading {file_path}: {e}");
                    false
                }
            }
        }));
    }

    let mut success = true;
    for handle in handles {
        success &= handle.await.unwrap_or(false);
    }

    if success {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
