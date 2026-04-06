# s3up

because there surely is need for another s3 uploader.  
content-type detection using mime_guess so solely based on file extension

## install

if you have rust/cargo installed on your system
`cargo install s3up`

otherwise download the appropriate binary from the [latest release](https://github.com/mxve/s3up/releases/latest/)

## usage

set env vars in either .env in work dir or in shell

```env
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_ENDPOINT_URL=
S3_BUCKET=
```

```bash
s3up <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> <file> # got it? add as many as you want. well actually maybe just consider using one of the battle tested tools that are like widely available and have been written by people who know what they are doing. this is just a library flip to be fair, but idk maybe i done goofed? who knows? maybe this is the best utility for uploading to an s3 bucket ever created. ein s3 uploader sie alle zu finden, ins dunkel zu treiben und ewig zu binden. im lande backblaze weil aws ist viel zu teuer lol wer bezahlt das?? why did i start speaking german all of a sudden i dont feel so well heh itll be fine i guess 影が動いてる、怖い
```

### args

- args go before files
- `--rename` - parse files as pairs of filename & s3_key
- `--concurrency <n>` - number of files to upload concurrently (default: 1)
- `--part-size <MB>` - multipart part size in megabytes (default: 16)
- `--part-concurrency <n>` - number of parts to upload concurrently per file (default: 16)

files over 32 MB are uploaded using multipart. parts are uploaded concurrently up to `--part-concurrency`.

```bash
s3up --rename --concurrency 8 local.txt remote.txt
```
