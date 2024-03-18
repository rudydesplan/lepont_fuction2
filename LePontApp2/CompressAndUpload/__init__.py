import logging
import azure.functions as func
from .main_logic import compress_and_upload_files

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Compression and upload process started.')
    compress_and_upload_files()
