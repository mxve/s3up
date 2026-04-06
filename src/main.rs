use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use std::io::SeekFrom;
use std::sync::Arc;
use std::{env, path::Path, process::ExitCode};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Semaphore;

#[cfg(test)]
mod tests;

const MULTIPART_THRESHOLD: u64 = 32 * 1024 * 1024;
const DEFAULT_PART_SIZE_MB: u64 = 16;
const DEFAULT_PART_CONCURRENCY: usize = 16;

fn require_env(key: &str) -> Option<String> {
    env::var(key)
        .map_err(|_| eprintln!("error: {key} not set"))
        .ok()
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MultipartConfig {
    pub part_size: u64,
    pub part_concurrency: usize,
}

#[derive(Debug)]
pub(crate) struct Args {
    pub concurrency: usize,
    pub multipart: MultipartConfig,
    pub uploads: Vec<(String, String)>,
}

pub(crate) fn parse_args(args: &[String]) -> Result<Args, String> {
    let mut rename = false;
    let mut concurrency = 1;
    let mut part_size = DEFAULT_PART_SIZE_MB * 1024 * 1024;
    let mut part_concurrency = DEFAULT_PART_CONCURRENCY;
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
            "--part-size" => {
                let val = args
                    .get(idx + 1)
                    .ok_or("error: --part-size requires a value")?;
                let mb = val.parse::<u64>().map_err(|_| {
                    format!("error: --part-size value must be a positive integer, got '{val}'")
                })?;
                if mb == 0 {
                    return Err("error: --part-size must be at least 1".into());
                }
                part_size = mb * 1024 * 1024;
                idx += 1;
            }
            "--part-concurrency" => {
                let val = args
                    .get(idx + 1)
                    .ok_or("error: --part-concurrency requires a value")?;
                part_concurrency = val.parse::<usize>().map_err(|_| {
                    format!(
                        "error: --part-concurrency value must be a positive integer, got '{val}'"
                    )
                })?;
                if part_concurrency == 0 {
                    return Err("error: --part-concurrency must be at least 1".into());
                }
                idx += 1;
            }
            _ => break,
        }
        idx += 1;
    }

    let file_args = &args[idx..];
    if file_args.is_empty() {
        return Err("usage: s3up [--rename] [--concurrency <n>] [--part-size <MB>] [--part-concurrency <n>] <file> [file2 ...]\n       s3up --rename [--concurrency <n>] [--part-size <MB>] [--part-concurrency <n>] <local_file> <s3_key> [<local_file> <s3_key> ...]".into());
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
        multipart: MultipartConfig {
            part_size,
            part_concurrency,
        },
        uploads,
    })
}

async fn upload_file(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    file_path: &str,
    key: &str,
    multipart: MultipartConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = Path::new(file_path);
    let file_size = tokio::fs::metadata(path).await?.len();
    let content_type = mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string();

    if file_size < MULTIPART_THRESHOLD {
        let body = ByteStream::from_path(path).await?;
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type(&content_type)
            .body(body)
            .send()
            .await?;
        return Ok(());
    }

    let create = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .content_type(&content_type)
        .send()
        .await?;
    let upload_id = create
        .upload_id()
        .ok_or("create_multipart_upload returned no upload_id")?;

    let result = upload_parts(client, bucket, key, upload_id, path, file_size, multipart).await;

    if let Err(e) = result {
        let _ = client
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        return Err(e);
    }

    Ok(())
}

async fn upload_parts(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    path: &Path,
    file_size: u64,
    multipart: MultipartConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let num_parts = file_size.div_ceil(multipart.part_size);
    let sem = Arc::new(Semaphore::new(multipart.part_concurrency));
    let client = Arc::new(client.clone());
    let bucket = Arc::new(bucket.to_owned());
    let key = Arc::new(key.to_owned());
    let upload_id = Arc::new(upload_id.to_owned());
    let path = Arc::new(path.to_owned());

    let mut handles = Vec::with_capacity(num_parts as usize);

    for part_number in 1..=num_parts {
        let offset = (part_number - 1) * multipart.part_size;
        let chunk_size = multipart.part_size.min(file_size - offset) as usize;

        let sem = sem.clone();
        let client = client.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        let upload_id = upload_id.clone();
        let path = path.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            let mut file = tokio::fs::File::open(&*path).await?;
            file.seek(SeekFrom::Start(offset)).await?;
            let mut buf = vec![0u8; chunk_size];
            file.read_exact(&mut buf).await?;

            let part = client
                .upload_part()
                .bucket(&*bucket)
                .key(&*key)
                .upload_id(&*upload_id)
                .part_number(part_number as i32)
                .body(ByteStream::from(buf))
                .send()
                .await?;

            Ok::<CompletedPart, Box<dyn std::error::Error + Send + Sync>>(
                CompletedPart::builder()
                    .part_number(part_number as i32)
                    .e_tag(part.e_tag().unwrap_or_default())
                    .build(),
            )
        }));
    }

    let mut completed_parts = Vec::with_capacity(handles.len());
    for handle in handles {
        completed_parts.push(handle.await??);
    }
    completed_parts.sort_unstable_by_key(|p| p.part_number());

    client
        .complete_multipart_upload()
        .bucket(&**bucket)
        .key(&**key)
        .upload_id(&**upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await?;

    Ok(())
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
        let multipart = args.multipart;

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let path = Path::new(&file_path);

            if !path.exists() {
                eprintln!("error: file not found: {file_path}");
                return false;
            }

            let content_type = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();

            match upload_file(&client, &bucket, &file_path, &key, multipart).await {
                Ok(_) => {
                    println!("uploaded: {file_path} -> {bucket}/{key} ({content_type})");
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
