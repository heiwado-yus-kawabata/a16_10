from fastapi import APIRouter
from pydantic import BaseModel

from logger import Logger

CONFIG_FILELIST_DIFF = "app/config/filelist_diff.txt"

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
    flag_diff = False
    with open(CONFIG_FILELIST_DIFF, "r") as file:
        for file_prefix in file:
            file_prefix = file_prefix.strip()
            if file_name.startswith(file_prefix):
                flag_diff = True
                break

    if flag_diff:
        logger.info("")
    else:
        logger.info("")

    return

