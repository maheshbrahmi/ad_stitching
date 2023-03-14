#https://github.com/Adori/Backend/blob/master/src/services/transcoding/__init__.py
# Standard Library Imports
import json
import os
import subprocess
import uuid

# Third Party Imports
from pydantic import ValidationError

# Imports from this repository
from utils.logger import get_logger

# Imports from this module
from .common import MediaFormat
from .common import MediaStream
from .common import ProbeData

logger = get_logger(__name__)


def _get_transcoded_file(input_audio_file):
    _dir = "/tmp/adori/keyword-spotter/"
    os.makedirs(_dir, exist_ok=True)
    temp_file_name = os.path.join(_dir, str(uuid.uuid4()) + ".mp3")
    subprocess.check_call(
        args=[
            "ffmpeg",
            "-hide_banner",
            "-nostats",
            "-loglevel",
            "panic",
            "-i",
            input_audio_file,
            "-ar",
            "44100",
            "-ac",
            "2",
            temp_file_name,
        ]
    )
    return temp_file_name


def _get_sample_rate(input_filename):
    _dir = "/tmp/adori/keyword-spotter/"
    os.makedirs(_dir, exist_ok=True)
    result = subprocess.run(
        args=[
            "ffprobe",
            "-hide_banner",
            "-v",
            "quiet",
            "-i",
            input_filename,
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
        ],
        stdout=subprocess.PIPE,
    )
    try:
        sample_rate = 0
        if result.returncode == 0:
            obj = json.loads(result.stdout)
            if isinstance(obj, dict):
                if "streams" in obj:
                    stream = obj["streams"][0]
                    sample_rate = int(stream["sample_rate"])
        else:
            logger.error(
                "FFProbe : Non-zero return code {}".format(result.returncode), return_code=result.returncode,
            )
    except json.JSONDecodeError:
        logger.error("Exception in Extracting Sample Rate", filname=input_filename, exc_info=True)
        sample_rate = 0

    if sample_rate == 0:
        raise RuntimeError("Unable to determine Sample Rate for {}".format(input_filename))

    return sample_rate


def do_transcode(input_filename):
    if (
        input_filename.startswith("http://")
        or input_filename.startswith("https://")
        or (os.path.isfile(input_filename) and os.path.exists(input_filename))
    ):
        return _get_transcoded_file(input_filename)
    else:
        logger.error("File Does not exist : {}".format(input_filename))


def get_sample_rate(input_filename: str = None):
    if (
        input_filename.startswith("http://")
        or input_filename.startswith("https://")
        or (os.path.isfile(input_filename) and os.path.exists(input_filename))
    ):
        return _get_sample_rate(input_filename)
    else:
        logger.error("File Does not exist : {}".format(input_filename))
        return None


def _get_format_info(input_filename) -> ProbeData:
    _dir = "/tmp/adori/keyword-spotter/"
    os.makedirs(_dir, exist_ok=True)
    result = subprocess.run(
        args=[
            "ffprobe",
            "-hide_banner",
            "-v",
            "quiet",
            "-i",
            input_filename,
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
        ],
        stdout=subprocess.PIPE,
    )

    try:
        if result.returncode == 0:
            obj = json.loads(result.stdout)
            probe_data: ProbeData = ProbeData(**obj)
            return probe_data
        else:
            logger.error("FFProbe : Non-zero return code {} ".format(result.returncode))
    except json.JSONDecodeError:
        logger.error(
            "Exception in json decoding ffmpeg info", filename=input_filename, exc_info=True,
        )
    except ValidationError:
        logger.error(
            "Exception in de-serializing json into media format", filename=input_filename, exc_info=True,
        )
    except Exception:
        logger.error(
            "Unhandled Exception in Extracting Media Format", filename=input_filename, exc_info=True,
        )

    raise RuntimeError("Unable to determine media format for ", input_filename)


def get_format_info(input_filename: str = None) -> ProbeData:
    if (
        input_filename.startswith("http://")
        or input_filename.startswith("https://")
        or (os.path.isfile(input_filename) and os.path.exists(input_filename))
    ):
        probe_data: ProbeData = _get_format_info(input_filename)
        return probe_data
    else:
        logger.error("File Does not exist : {}".format(input_filename))