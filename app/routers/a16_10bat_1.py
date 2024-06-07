import io
import os

from fastapi import APIRouter
from google.cloud import storage
from pydantic import BaseModel

from logger import Logger

CONFIG_FILELIST_DIFF = "../../config/filelist_diff.txt"
DST_FILE_FORMAT = "{0}_PART_{1}.{2}"

router = APIRouter(prefix="/1split")
logger = Logger(log_name="a16_10bat")


class RequestBody(BaseModel):
    ProjectId: str
    Bucket: str
    FilePath: str


@router.post("/")
async def a16_10bat_1(body: RequestBody):

    # 差分ファイル情報の取得
    # config_path = os.path.join(os.path.dirname(__file__), CONFIG_FILELIST_DIFF)
    # logger.info(config_path)

    # 対象ファイル名
    file_name = body.FilePath

    # ファイル名判定 -> 差分連携ではないなら1ファイルをそのまま置く
    flag_diff = False
    with open(config_path, "r") as file:
        for file_prefix in file:
            file_prefix = file_prefix.strip()
            if file_name.startswith(file_prefix):
                flag_diff = True
                break

    bucket_name = body.Bucket
    if flag_diff:
        split_file(bucket_name, file_name)
    else:
        copy_file(bucket_name, file_name)

    return


def split_file(bucket_name: str, file_name: str):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    memory_buf = io.StringIO()

    with blob.open("r") as fs:
        for line in fs:
            memory_buf.write(line)

    return


def copy_file(bucket_name: str, file_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # コピー先のオブジェクト
    filenames = file_name.split(".")
    dst_filename = DST_FILE_FORMAT.format(filenames[0], 1, filenames[1])
    dst_blob = bucket.blob(dst_filename)

    src_blob = bucket.blob(file_name)
    bucket.copy_blob(src_blob, bucket, dst_blob.name)

    logger.info(f"{dst_blob.name}生成")
