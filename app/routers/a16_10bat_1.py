import io
import os

from fastapi import APIRouter
from google.cloud import storage
from pydantic import BaseModel

from logger import Logger

CONFIG_FILELIST_DIFF = "/app/config/filelist_diff.txt"
DST_FILE_FORMAT = "{0}_PART_{1}.{2}"
SEPARATE_COUNT = 2000000

router = APIRouter(prefix="/1split")
logger = Logger(log_name="a16_10bat")


class RequestBody(BaseModel):
    ProjectId: str
    Bucket: str
    FilePath: str


@router.post("/")
async def a16_10bat_1(body: RequestBody):

    # 対象ファイル名
    file_name = body.FilePath

    # ファイル名判定 -> 差分連携ではないなら1ファイルをそのまま置く
    encoding = None
    with open(CONFIG_FILELIST_DIFF, "r") as file:
        for line in file:
            file_prefix, file_encoding = line.strip().split(',')
            if file_name.startswith(file_prefix.strip()):
                encoding = file_encoding
                logger.info(f"差分連携ファイル: {file_prefix} with encoding: {encoding}")
                break

    bucket_name = body.Bucket
    if encoding is not None:
        split_file(bucket_name, file_name, encoding)
    else:
        copy_file(bucket_name, file_name)

    return


def split_file(bucket_name: str, file_name: str, file_encoding: str):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    memory_buf = io.StringIO()
    index_file = 1
    index_line = 1

    with blob.open("rt", encoding=file_encoding) as file_stream:
        for line in file_stream:

            logger.mem_usage(index_line)

            if index_line < SEPARATE_COUNT:
                # 指定行数以下であればメモリに書き込み
                memory_buf.write(line)
                index_line += 1
            else:
                # 指定行数を越えた時点で分割してファイル出力
                split_filenames = file_name.split(".")
                dst_filename = DST_FILE_FORMAT.format(split_filenames[0], index_file, split_filenames[1])
                dst_blob = bucket.blob(dst_filename)
                dst_blob.upload_from_string(memory_buf.getvalue())
                logger.info(f"{dst_blob.name}生成")

                # ファイルインデックスを増やす
                index_file += 1

                # 他はクリア
                index_line = 1
                memory_buf.seek(0)
                memory_buf.truncate()

    # 残りをアップロード
    if memory_buf.tell() > 0:
        split_filenames = file_name.split(".")
        dst_filename = DST_FILE_FORMAT.format(split_filenames[0], index_file, split_filenames[1])
        dst_blob = bucket.blob(dst_filename)
        dst_blob.upload_from_string(memory_buf.getvalue())
        logger.info(f"{dst_blob.name}生成")

    memory_buf.close()

    return


def copy_file(bucket_name: str, file_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # コピー先のオブジェクト
    split_filenames = file_name.split(".")
    dst_filename = DST_FILE_FORMAT.format(split_filenames[0], 1, split_filenames[1])
    dst_blob = bucket.blob(dst_filename)

    src_blob = bucket.blob(file_name)
    bucket.copy_blob(src_blob, bucket, dst_blob.name)

    logger.info(f"{dst_blob.name}生成")
