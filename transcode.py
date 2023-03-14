import hashlib
import json
import os
import sys
import subprocess
import uuid
import logging
import shutil
from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import List
from typing import Tuple

# Third Party Imports
import ffmpeg
import requests

# Imports from this repository
#sys.path.insert(1, "/workspaces/Adorify/pyadorifier")
sys.path.append("/workspaces/Adorify")
from pyadorifier.utils.logger import get_logger
from pyadorifier.utils.adorify import run_pb_adorifier

logger = get_logger(__name__)


#This is a transcode written for testing MP3 files and ad stitching
# Step 1 transcodes the MP3 file to the following output
    # adori audio std 
    # stereo
    # 44100 Hz
    # 16 bit
    # 128kbps CBR
# Step 2 PBencode


# Calls run_pb_adorifier  from pyadorifier.utils.adorify and returns the adorified file with "-pb" appended to 
# the input filename. Make sure pbadorifier binary PATH is present in the environment
# The caller of the function is required to delete any remaining input file(s) in the tmp folder.
def do_pbadorify(input_filename: str, output_filename: str, adoriId: int):
    if os.path.exists(input_filename):
        print(f"PBencoding {input_filename}")
        run_pb_adorifier(input_filename,output_filename,adoriId)
        return True
    else:
        return False

def do_transcode(input_filename: str, output_filename: str):
    if os.path.exists(input_filename):
        try:                
            print(f"tanscoding {input_filename}")
            subprocess.check_call(
                args=[
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-nostats",
                    "-loglevel",
                    "panic",
                    "-i",
                    input_filename,
                    "temp.wav",
                ]
            )
            subprocess.check_call(
                args=[
                    "ffmpeg",
                    "-y",
                    "-hide_banner",
                    "-nostats",
                    "-loglevel",
                    "panic",
                    "-i",
                    "temp.wav",
                    "-ar",
                    "44100",
                    "-ac",
                    "2",
                    "-b:a", 
                    "128k",
                    output_filename,
                ]
            )
            if os.path.exists("temp.wav"): os.remove("temp.wav")
            return True 
        except ffmpeg.Error as e:
            logger.error("ffmpeg.probe do_transcode : {}".format(e.stderr))
            return False
    else:
        logger.error("File Does not exist : {}".format(input_filename))
        return False


# need to build Binaries and add to the PATH   
# goto adorify folder and make all
# then add the build folder path to the environment 
# export PATH="/workspaces/Adorify/adorify/build/:$PATH"
#Example Usage: python3 transcode.py test/covid_promo.mp3 test/covid_tc.mp3 121
def main(argv):
    if (len(sys.argv)<4):
        print("Usage: transcode input_filename output_filename adoriID")
    else:
        input_filename = sys.argv[1]
        output_filename = sys.argv[2]
        adoriId = int(sys.argv[3])
        print(f" input_filename: {input_filename}\n output_filename: {output_filename}\n adoriId: {adoriId}\n")
    if os.path.exists(input_filename):
        result =  do_transcode(input_filename,"transcoded.mp3")
        if(result==True):
            do_pbadorify("transcoded.mp3", output_filename,  adoriId)
        if os.path.exists("transcoded.mp3"): os.remove("transcoded.mp3")
    else:
        print("file not found")


if __name__ == "__main__":
    main(sys.argv[1:])
