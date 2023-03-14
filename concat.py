import hashlib
import json
import os
import sys
import subprocess
import uuid
import logging
import shutil
import logger
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
#sys.path.append("d:/ad_stitching")
# from pyadorifier.utils.logger import get_logger
# from pyadorifier.utils.adorify import run_pb_adorifier

# logger = get_logger(__name__)

path_tmp = 'd:/ad_stitching/tmp/'

#Note: STEP1-3 is removed. Its assumed that the incoming files are transcoded and adorified.
## Step 1 check if the audio file needs transcoding
    # adori audio std 
    # stereo
    # 44100 Hz
    # 16 bit
    # 128kbps CBR
## if so transcode it to std format
## Step 2 remove ID3 Tags from the non-track files 
## Step 3 Perform a PB encode of all the file


# Step 4 Splice and Concatanate the files
def GetAudioLength(inputFile: str = None):
    if not os.path.exists(inputFile): 
        print(f"Audio file {inputFile} does not exist")
        return 0
    result = subprocess.run(
        args=[
            "ffprobe",
            "-hide_banner",
            "-v",
            "quiet",
            "-i",
            inputFile,
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
        ],
        stdout=subprocess.PIPE,
    )
    try:
        audio_length = 0
        if result.returncode == 0:
            obj = json.loads(result.stdout)
            if isinstance(obj, dict):
                if "streams" in obj:
                    stream = obj["streams"][0]
                    audio_length = float(stream["duration"])
        else:
            logging.error("FFProbe : Non-zero return code {}")
    except json.JSONDecodeError:
        logging.error(json.JSONDecodeError)
        audio_length = 0

    if audio_length == 0:
        raise RuntimeError(
            "Unable to determine duration for {}".format(inputFile)
        )
    return audio_length

#.global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
def _get_concat_files(basename: str, orig_file: str, insert_segments: List[Dict]):
    concat_files = []
    prev_input_slice_end = 0
    outro = False
    for ad in insert_segments:
        mark_in_sec = ad["markInMillis"] / 1000.0
        if mark_in_sec == 0:
            prev_input_slice_end = mark_in_sec
            concat_files.append(ad["filepath"])
        elif mark_in_sec < 0:
             outro = True
             outrofile = ad["filepath"]
        elif prev_input_slice_end < mark_in_sec:
            output_file = f"{path_tmp}{basename}-{prev_input_slice_end}-{mark_in_sec}.mp3"
            try:
                cmd = (
                    ffmpeg.input(orig_file, ss=prev_input_slice_end, to=mark_in_sec)
                    .output(output_file, c="copy", avoid_negative_ts=1)
                    .global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
                )
                cmd.run(capture_stdout=True, capture_stderr=True)
                prev_input_slice_end = mark_in_sec
                concat_files.append(output_file)
                concat_files.append(ad["filepath"])
            except ffmpeg.Error as e:
                print("Unable to stitch files"+ str(e))
                print('stdout:', e.stdout.decode('utf8'))
                print('stderr:', e.stderr.decode('utf8'))
                logger.error("ffmpeg.concat : {}".format(e.stderr))  
                raise e
        
    output_file = f"{path_tmp}{basename}-{prev_input_slice_end}-end.mp3"
    cmd = (
        ffmpeg.input(orig_file, ss=prev_input_slice_end)
        .output(output_file, c="copy", avoid_negative_ts=1)
        .global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
    )
    cmd.run()
    concat_files.append(output_file)

    if outro == True:
        concat_files.append(outrofile)

    return concat_files

# This function takes the original file, ad_segments and a uid for unique naming of the output file. 
# This is done in two steps. 
# Step 1: The track_file(orig_file) is sliced according to the markers, the sliced files and the ad segments are 
# placed in a txt file as show below:
# file '/workspaces/Adorify/pyadorifier/ad_stitching/tmp/someuid-13fe4cab-7ddf-4204-a6d9-f1bdb6f0b745-0-12.0.mp3'
# file '/workspaces/Adorify/pyadorifier/ad_stitching/tmp/Coke_tc_pb.mp3'
# file '/workspaces/Adorify/pyadorifier/ad_stitching/tmp/someuid-13fe4cab-7ddf-4204-a6d9-f1bdb6f0b745-12.0-22.0.mp3'
# file '/workspaces/Adorify/pyadorifier/ad_stitching/tmp/kfc128_tc_pb.mp3'
# file '/workspaces/Adorify/pyadorifier/ad_stitching/tmp/someuid-13fe4cab-7ddf-4204-a6d9-f1bdb6f0b745-22.0-end.mp3'
# Step 2: The files are stitched together using this FFMPEG subprocess
#  ffmpeg -f concat -i list.txt -c copy out.mp3
 #.global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
def do_concat_files(uid: str, orig_file: str, insert_segments: List[Dict]):
    basename = f"{uid}-{uuid.uuid4()}"
    concat_file_list = _get_concat_files(basename, orig_file, insert_segments)
    concat_filename = f"{path_tmp}{basename}-concat-list.txt"
    with open(concat_filename, "w") as f:
        for filename in concat_file_list:
            f.write(f"file '{filename}'\n")

    output_filename = f"{path_tmp}{basename}.mp3"
    # exceptions are not raised when an invalid or non existing file is present???
    try:
        cmd = (
            ffmpeg.input(concat_filename, f="concat", safe=0)
            .output(output_filename, c="copy")
            .global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
        )
        cmd.run(capture_stdout=True, capture_stderr=True)
    except Exception as e:
        print("Unable to stitch files"+ str(e))
        print('stdout:', e.stdout.decode('utf8'))
        print('stderr:', e.stderr.decode('utf8'))
    finally:
        # remove temp files created within this method
        os.remove(concat_filename)
        #print (f"removing {concat_filename}")
        # for seg in insert_segments:
        #     concat_file_list.remove(seg["filepath"])
        for f in concat_file_list: 
            os.remove(f)
            #print (f"removing {f}")

    return output_filename



def _get_file_segments(basename: str, orig_file: str, remove_segments: List[Dict]):
    concat_files = []
    prev_input_slice_end = 0
    for ad in remove_segments:
        mark_in_sec = ad["markInMillis"] / 1000.0
        duration = ad["duration"] / 1000.0
        if mark_in_sec == 0:
            prev_input_slice_end = mark_in_sec + duration
            # concat_files.append(ad["filepath"])
        elif prev_input_slice_end < mark_in_sec:
            output_file = f"{path_tmp}{basename}-{prev_input_slice_end}-{mark_in_sec}.mp3"
            try:
                cmd = (
                    ffmpeg.input(orig_file, ss=prev_input_slice_end, to=mark_in_sec)
                    .output(output_file, c="copy", avoid_negative_ts=1)
                    .global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
                )
                cmd.run(capture_stdout=True, capture_stderr=True)
                prev_input_slice_end = mark_in_sec + duration
                concat_files.append(output_file)
                #concat_files.append(ad["filepath"])
            except ffmpeg.Error as e:
                print("Unable to stitch files"+ str(e))
                print('stdout:', e.stdout.decode('utf8'))
                print('stderr:', e.stderr.decode('utf8'))
                logger.error("ffmpeg.concat : {}".format(e.stderr))  
                raise e
        
    output_file = f"{path_tmp}{basename}-{prev_input_slice_end}-end.mp3"
    cmd = (
        ffmpeg.input(orig_file, ss=prev_input_slice_end)
        .output(output_file, c="copy", avoid_negative_ts=1)
        .global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
    )
    cmd.run()
    concat_files.append(output_file)

    return concat_files


# This function takes the original file, ad_segments and a uid for unique naming of the output file. 
# This is done in two steps. 
# Step 1: The track_file(orig_file) is sliced according to the start_time and duration, the sliced files and the ad segments are 
# placed in a txt file as show below:
# file '/workspaces/Adorify/pyadorifier/ad_stitching/tmp/someuid-13fe4cab-7ddf-4204-a6d9-f1bdb6f0b745-0-12.0.mp3'
# file '/workspaces/Adorify/pyadorifier/ad_stitching/tmp/someuid-13fe4cab-7ddf-4204-a6d9-f1bdb6f0b745-12.0-22.0.mp3'
# file '/workspaces/Adorify/pyadorifier/ad_stitching/tmp/someuid-13fe4cab-7ddf-4204-a6d9-f1bdb6f0b745-22.0-end.mp3'
# Step 2: The files are stitched together using this FFMPEG subprocess
#  ffmpeg -f concat -i list.txt -c copy out.mp3
 #.global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
def do_remove_ads(uid: str, orig_file: str, remove_segments: List[Dict]):
    basename = f"{uid}-{uuid.uuid4()}"
    concat_file_list = _get_file_segments(basename, orig_file, remove_segments)
    concat_filename = f"{path_tmp}{basename}-concat-list.txt"
    with open(concat_filename, "w") as f:
        for filename in concat_file_list:
            f.write(f"file '{filename}'\n")

    output_filename = f"{path_tmp}{basename}.mp3"
    # exceptions are not raised when an invalid or non existing file is present???
    try:
        cmd = (
            ffmpeg.input(concat_filename, f="concat", safe=0)
            .output(output_filename, c="copy")
            .global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
        )
        cmd.run(capture_stdout=True, capture_stderr=True)
    except Exception as e:
        print("Unable to stitch files"+ str(e))
        print('stdout:', e.stdout.decode('utf8'))
        print('stderr:', e.stderr.decode('utf8'))
    finally:
        # remove temp files created within this method
        os.remove(concat_filename)
        #print (f"removing {concat_filename}")
        # for seg in insert_segments:
        #     concat_file_list.remove(seg["filepath"])
        for f in concat_file_list: 
            os.remove(f)
            #print (f"removing {f}")

    return output_filename

# def remove_metadata(input_filename: str, output_filename: str):
# #ffmpeg -i in.mp3 -codec:a copy -map_metadata -1 out.mp3
#     subprocess.check_call(
#         args=[
#             "ffmpeg",
#             "-y",
#             "-hide_banner",
#             "-nostats",
#             "-loglevel",
#             "panic",
#             "-i",
#             input_filename,
#             "-codec:a",
#             "copy",
#             "-map_metadata",
#             "-1",
#             output_filename,
#         ]
#     )
#     return output_filename

def append_txtToFilename(input_filename: str, txt: str ):
    _tmpdir = path_tmp
    os.makedirs(_tmpdir, exist_ok=True)
    if os.path.exists(input_filename):
        _, tail = os.path.split(input_filename) # gets filename+extension only
        name, ext = os.path.splitext(tail)
        output_filename = f"{_tmpdir}{name}{txt}{ext}"
        return output_filename 



# Runs ffprobe on the file to check if each of the following criteria is met
#    Stereo
#    44100 Hz
#    16 bit
#    128kbps CBR
# If they are met then transcoding is not done, otherwise the file  is transcoded to the above format using FFMPEG
# ffmpeg -i in.mp3 -f mp3 -ar 44100 -ac 2 -b:a 128k out.mp3
# For non-track(ad/intro/outro) file the metadata is removed by calling the following subprocess 
# (This feature has been disabled for now - PBencoding removes the metadata) 
# ffmpeg -i in.mp3 -codec:a copy -map_metadata -1 out.mp3
# After transcoding is done the output file is placed in the tmp path and is appended with _tc to its file name.
# This function returns the final transcoded file.
# The caller of the function is required to delete any remaining input file(s) in the tmp folder.
def do_transcode(input_filename: str, remove_meta: bool = False):
    if os.path.exists(input_filename):
        output_filename = append_txtToFilename(input_filename, "_tc")
        try:
            probe = ffmpeg.probe(input_filename)
            for stream in probe['streams']:
                if stream['codec_type'] == 'audio':
                    if stream['channels'] == 2 and stream['bit_rate'] == '128000' and stream['sample_rate'] == '44100' : #transcode not needed
                        # if(remove_meta):
                        #     output_filename = remove_metadata(input_filename,output_filename)
                        # else:
                        shutil.copy(input_filename,output_filename) # for future, changing copy to move can save time, but you will loose the original file
                    else: # transcode needed
                        if(remove_meta):
                            print(f"tanscoding & REMOVING META {input_filename}")
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
                            os.remove("temp.wav")
                        else:
                            print(f"tanscoding only {input_filename}")
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
                                    "-ar",
                                    "44100",
                                    "-ac",
                                    "2",
                                    "-b:a", 
                                    "128k",
                                    output_filename,
                                ]
                            )
                return output_filename 
        except ffmpeg.Error as e:
            logger.error("ffmpeg.probe do_transcode : {}".format(e.stderr))   
    else:
        logger.error("File Does not exist : {}".format(input_filename))

def get_StitchedDuration(objectDictionary: dict):
    #objectDictionary = json.loads(objectJson)
    track_file = objectDictionary['track_file']
    if not os.path.exists(track_file): 
        print(f"Track segment {track_file} does not exist")
        return 0
    total_duration = GetAudioLength(track_file)
    print(f"Duration of {track_file} = {total_duration} Secs")
    ad_segments = objectDictionary['ad_segments']
    for ad in ad_segments:
        if not os.path.exists(ad["filepath"]): 
            print(f"Ad segment {ad['filepath']} does not exist")
            return 0
        duration = GetAudioLength(ad["filepath"])
        total_duration += duration
        print(f"Duration of {ad['filepath']} = {duration} Secs")
    return total_duration


def stitch_ads(_uid: str, objectDictionary:  dict):
    #objectDictionary = json.loads(inputJson)
    track_file = objectDictionary['track_file']
    #Step 2 Concatenate all files
    ad_segments = objectDictionary['ad_segments']
    try:
        concatenated_file = do_concat_files(_uid, track_file, ad_segments)
        return concatenated_file
    except subprocess.CalledProcessError as e:
        print("Unable to stitch files"+ str(e))

def remove_ads(_uid: str, objectDictionary:  dict):
    #objectDictionary = json.loads(inputJson)
    track_file = objectDictionary['track_file']
    #Step 2 Concatenate all files
    ad_segments = objectDictionary['ad_segments']
    try:
        concatenated_file = do_remove_ads(_uid, track_file, ad_segments)
        return concatenated_file
    except subprocess.CalledProcessError as e:
        print("Unable to stitch files"+ str(e))




# need to build Binaries and add to the PATH   
# goto adorify folder and make all
# then add the build folder path to the environment 
# export PATH="/workspaces/Adorify/adorify/build/:$PATH"
def main(argv):
    if len(argv) > 1:
        with open(sys.argv[1]) as json_data:
            objectJson = json.load(json_data)
            print(objectJson)
            objectDictionary = objectJson
    _uid = "someuid"
    if not os.path.exists('tmp'):
        os.makedirs('tmp')
    cwd = os.getcwd()
    global path_tmp
    path_tmp = cwd+"\\tmp\\"
    print(path_tmp)
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print(dir_path)
    

    # objectJson = '{ \
    #     "track_file" : "ivm_episode1.mp3",\
    #     "cmd": "stitch",\
    #     "ad_segments" : [ \
    #     {"markInMillis": 5000, "filepath": "kfc.mp3"},\
    #     {"markInMillis": 40000, "filepath": "Geico.mp3"},\
    #     {"markInMillis": 70000, "filepath": "Coke.mp3"}\
    #     ] }'

    objectJson = '{ \
        "track_file" : "ivm_episode1.mp3",\
        "cmd": "remove",\
        "ad_segments" : [ \
        {"markInMillis": 0, "duration": 20000},\
        {"markInMillis": 40000, "duration": 20000},\
        {"markInMillis": 70000, "duration": 20000}\
        ] }'

    objectDictionary = json.loads(objectJson)
  
    cmd = objectDictionary['cmd']
    if cmd=="stitch":
        stitched_duration = get_StitchedDuration(objectDictionary)
        stitched_duration = 10
        track_file = objectDictionary['track_file']
        print(f"track_file =  {track_file}")
        ad_segments = objectDictionary['ad_segments']
        for i, ad in enumerate(ad_segments):
            if not os.path.exists(ad["filepath"]): 
                print(f"Ad segment {ad['filepath']} does not exist")
                return 0
            print(f"ad_file[{i}] =  {ad['filepath']}") 
            #copy file to temp folder and update the object
            copied_file = shutil.copy(ad['filepath'], path_tmp)
            ad["filepath"] = copied_file
            print(f"ad_file[{i}] new =  {ad['filepath']}") 

        print(f"ad_segments[0] filepath=  {objectDictionary['ad_segments'][0]['filepath']}")
        start_time = datetime.utcnow().timestamp()

        concatenated_file = stitch_ads(_uid, objectDictionary)
        
        print(f"Code Execution Time {datetime.utcnow().timestamp() - start_time} Secs")

        #Sanity check
        print(f"Estimated stitched duration is {stitched_duration} Secs")
        duration = GetAudioLength(concatenated_file)
        print(f"duration of {concatenated_file} is {duration} Secs")
    elif cmd=="remove":
        start_time = datetime.utcnow().timestamp()
        concatenated_file = remove_ads(_uid, objectDictionary)
        print(f"Code Execution Time {datetime.utcnow().timestamp() - start_time} Secs")
        #Sanity check
        #print(f"Estimated stitched duration is {stitched_duration} Secs")
        duration = GetAudioLength(concatenated_file)
        print(f"duration of {concatenated_file} is {duration} Secs")
# Remove Ads
def main1(argv):
    _uid = "someuid"
    if not os.path.exists('tmp'):
        os.makedirs('tmp')

    cwd = os.getcwd()
    global path_tmp
    path_tmp = cwd+"\\tmp\\"
    print(path_tmp)
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print(dir_path)
    objectJson1 = '{ \
        "track_file" : "ivm_episode1.mp3",\
        "cmd": "remove",\
        "ad_segments" : [ \
        {"markInMillis": 0, "duration": 20000},\
        {"markInMillis": 40000, "duration": 20000},\
        {"markInMillis": 70000, "duration": 20000}\
        ] }'
    objectDictionary = json.loads(objectJson1)
    start_time = datetime.utcnow().timestamp()
    concatenated_file = remove_ads(_uid, objectDictionary)
    print(f"Code Execution Time {datetime.utcnow().timestamp() - start_time} Secs")
    #Sanity check
    #print(f"Estimated stitched duration is {stitched_duration} Secs")
    duration = GetAudioLength(concatenated_file)
    print(f"duration of {concatenated_file} is {duration} Secs")

# usage: python concat.py .\remove.json
# usage: python concat.py .\stitch.json
# mp4 to mp3 conversion ffmpeg -i video.mp4 -vn -sn -c:a mp3 -ab 192k audio.mp3
if __name__ == "__main__":
    main(sys.argv[1:])

# Research and notes
# ffplay -nodisp -autoexit kfc.mp3
# ffmpeg -i originalA.mp3 -f mp3 -ar 44100 -ac 2 -b:a 128k intermediateA.mp3  // -ac Set the number of audio channels. 
# If you need constant bitrate (CBR) MP3 audio, you need to use the -b:a option instead of -qscale:a. Here you can specify 
# the number of bits per second, for example -b:a 256k if you want 256 Kbit/s (25.6 KB/s) audio. Available options 
# are: 8, 16, 24, 32, 40, 48, 64, 80, 96, 112, 128, 160, 192, 224, 256, or 320 (add a k after each to get that rate).
# adori audio std 
# stereo
# 44100 Hz
# 16 bit
# 128kbps CBR
# ffprobe -v quiet -print_format json -show_format -show_streams example.mp3
#ffmpeg -i input.mp3 -codec:a libmp3lame -b:a 128k output.mp3

# ffprobe -v quiet -print_format json -show_format -show_streams example.mp3

# {
#     "streams": [
#         {
#             "index": 0,
#             "codec_name": "mp3",
#             "codec_long_name": "MP3 (MPEG audio layer 3)",
#             "codec_type": "audio",
#             "codec_time_base": "1/11025",
#             "codec_tag_string": "[0][0][0][0]",
#             "codec_tag": "0x0000",
#             "sample_fmt": "s16p",
#             "sample_rate": "11025",
#             "channels": 1,
#             "channel_layout": "mono",
#             "bits_per_sample": 0,
#             "r_frame_rate": "0/0",
#             "avg_frame_rate": "0/0",
#             "time_base": "1/14112000",
#             "start_pts": 0,
#             "start_time": "0.000000",
#             "duration_ts": 55294344,
#             "duration": "3.918250",
#             "bit_rate": "32000",
#             "disposition": {
#                 "default": 0,
#                 "dub": 0,
#                 "original": 0,
#                 "comment": 0,
#                 "lyrics": 0,
#                 "karaoke": 0,
#                 "forced": 0,
#                 "hearing_impaired": 0,
#                 "visual_impaired": 0,
#                 "clean_effects": 0,
#                 "attached_pic": 0,
#                 "timed_thumbnails": 0
#             }
#         }
#     ],
#     "format": {
#         "filename": "example.mp3",
#         "nb_streams": 1,
#         "nb_programs": 0,
#         "format_name": "mp3",
#         "format_long_name": "MP2/3 (MPEG audio layer 2/3)",
#         "start_time": "0.000000",
#         "duration": "3.918250",
#         "size": "17260",
#         "bit_rate": "35240",
#         "probe_score": 51,
#         "tags": {
#             "title": "Sound Effects - Female Operatic La 1 - Opera singer sings La.",
#             "artist": "Download Sound Effects - SoundDogs - AOS",
#             "album": "http://www.Sounddogs.com",
#             "track": "0",
#             "copyright": "(c) 2010 Sounddogs.com, All Rights Reserved",
#             "genre": "SFX - Humans; Vocalizations",
#             "comment": "Royalty Free Sound Effects - Sounddogs.com",
#             "date": "2008"
#         }
#     }
# }
# file '/path/to/first.mp3'
# file '/path/to/second.mp3'
# and then

# ffmpeg -f concat -i list.txt -c copy out.mp3


# start_time = datetime.utcnow().timestamp()
# print("Duration", datetime.utcnow().timestamp() - start_time)
# meta = {
#     'channels': stream['channels'],
#     'bitrate' : int(stream['bit_rate']),
#     'sample_rate': int(stream['sample_rate']),
#     'duration': float(probe['format']['duration'])
# }
# return meta
# for line in sys.path:
#     print (line)
# os.environ["pbadorifier"] = "/workspaces/Adorify/adorify/build/pbadorifier"
# print(os.environ['pbadorifier'])
#os.system('pbadorifier')
