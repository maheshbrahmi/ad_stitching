# Created by vinod on 19 Jan, 2021
# Standard Library Imports
import hashlib
import json
import os
import subprocess
import uuid
from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import List
from typing import Tuple

# Third Party Imports
import ffmpeg
import requests
from adori.db import models

# Imports from this repository
from crud.networks.utils import get_audio_length
from docs.api.utils.db import db_session_maker
from services.storage import audio_bucket
from utils.http import file_ext_from_content_type_header
from utils.http import get_etag_from_header
from utils.logger import get_logger

logger = get_logger(__name__)


def download_audio(url: str) -> Tuple[str, str, datetime]:
    local_filename = None
    etag = ""
    expiry = datetime.utcnow() + timedelta(hours=6)

    try:
        resp = requests.get(url, stream=True, timeout=10)
        resp.raise_for_status()
        etag, expiry = get_etag_from_header(resp.headers)

        # 'audio/mpeg' will be identified as mp2. Correct it to mp3
        ext = file_ext_from_content_type_header(resp.headers.get("Content-Type", None))
        if not ext or ext == ".mp2":
            ext = ".mp3"

        local_filename = f"/tmp/{str(uuid.uuid4())}{ext}"
        with open(local_filename, "wb") as fd:
            for chunk in resp.iter_content(1024):
                if chunk:
                    fd.write(chunk)
        return local_filename, etag, expiry
    except requests.exceptions.HTTPError:
        logger.error("Http Error", url=url, exc_info=True)
    except requests.exceptions.ConnectionError:
        logger.error("Connection Error", url=url, exc_info=True)
    except requests.exceptions.Timeout:
        logger.error("Timeout Error", url=url, exc_info=True)
    except requests.exceptions.RequestException:
        logger.error("Unknown Error", url=url, exc_info=True)

    return local_filename, etag, expiry


def _get_concat_files(basename: str, orig_file: str, insert_segments: List[Dict]):
    concat_files = []
    prev_input_slice_end = 0
    for idx, ad in enumerate(insert_segments):
        mark_in_sec = ad["markInMillis"] / 1000.0
        if prev_input_slice_end < mark_in_sec:
            output_file = f"/tmp/{basename}-{prev_input_slice_end}-{mark_in_sec}.mp3"
            cmd = (
                ffmpeg.input(orig_file, ss=prev_input_slice_end, to=mark_in_sec)
                .output(output_file, c="copy", avoid_negative_ts=1)
                .global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
            )
            cmd.run()
            prev_input_slice_end = mark_in_sec
            concat_files.append(output_file)
        concat_files.append(ad["filepath"])

    output_file = f"/tmp/{basename}-{prev_input_slice_end}-end.mp3"
    cmd = (
        ffmpeg.input(orig_file, ss=prev_input_slice_end)
        .output(output_file, c="copy", avoid_negative_ts=1)
        .global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
    )
    cmd.run()
    concat_files.append(output_file)

    return concat_files


def _concat_files(uid: str, orig_file: str, insert_segments: List[Dict]):
    basename = f"{uid}-{uuid.uuid4()}"
    concat_file_list = _get_concat_files(basename, orig_file, insert_segments)

    concat_filename = f"/tmp/{basename}-concat-list.txt"
    with open(concat_filename, "w") as f:
        for filename in concat_file_list:
            f.write(f"file '{filename}'\n")

    now = datetime.utcnow().timestamp()
    output_filename = f"/tmp/{basename}.mp3"

    cmd = (
        ffmpeg.input(concat_filename, f="concat", safe=0)
        .output(output_filename, c="copy")
        .global_args("-nostats", "-y", "-hide_banner", "-v", "quiet")
    )
    cmd.run()

    # remove temp files created within this method
    os.remove(concat_filename)
    for seg in insert_segments:
        concat_file_list.remove(seg["filepath"])
    for f in concat_file_list:
        os.remove(f)

    return output_filename


def _stitch_files_without_encoding(track: models.AudioTrack, track_audio_ads: List[models.AudioTrackAd]):
    blob = audio_bucket.get_blob(track.urlSuffix)
    file_url = blob.generate_signed_url(expiration=timedelta(hours=1))
    adorified_file_path, _, _ = download_audio(file_url)

    insert_segments = []
    for ad in track_audio_ads:
        blob = audio_bucket.get_blob(ad.audioAd.get_file_path())
        url = blob.generate_signed_url(expiration=timedelta(hours=1))
        local_filepath, _, _ = download_audio(url)
        insert_segments.append({"markInMillis": ad.markInMillis, "filepath": local_filepath})

    try:
        output_file = _concat_files(f"{track.id or ''}-{track.uid}", adorified_file_path, insert_segments)
        duration = get_audio_length(output_file)
        duration_millis = duration * 1000
        return output_file, duration_millis
    finally:
        # remove all temp files
        os.remove(adorified_file_path)
        for seg in insert_segments:
            os.remove(seg["filepath"])


def _get_ffmpeg_filters_and_taps(audio_ads: List[models.AudioTrackAd]):
    n_input_slice = 0
    prev_input_slice_end = 0

    trim_filters = []
    trim_filter_taps = []

    for idx, ad in enumerate(audio_ads):
        mark_in_sec = ad.markInMillis / 1000.0
        if prev_input_slice_end < mark_in_sec:
            tap = "[input{}]".format(n_input_slice)
            filter_str = "[0:a]atrim=start={}".format(prev_input_slice_end)
            filter_str += ":end={},".format(mark_in_sec)
            filter_str += "asetpts=PTS-STARTPTS{}".format(tap)
            trim_filters.append(filter_str)
            trim_filter_taps.append(tap)
            n_input_slice += 1
            prev_input_slice_end = mark_in_sec
        tap = "[ad{}]".format(idx)
        filter_str = "[{}:a]anull{}".format(idx + 1, tap)
        trim_filters.append(filter_str)
        trim_filter_taps.append(tap)

    tap = "[input{}]".format(n_input_slice)
    filter_str = "[0:a]atrim=start={},".format(prev_input_slice_end)
    filter_str += "asetpts=PTS-STARTPTS{}".format(tap)
    trim_filters.append(filter_str)
    trim_filter_taps.append(tap)

    return trim_filters, trim_filter_taps


def _stitch_files_with_encoding(track: models.AudioTrack, track_audio_ads: List[models.AudioTrackAd]):

    for ad in track_audio_ads:
        if ad.audioAd.adSourceId == models.AdServiceSource.THIRD_PARTY:
            raise ValueError("Only static ads are supported for stitching")

    stitched_file_name = f"/tmp/{track.id or ''}-{track.uid}-{uuid.uuid4()}.mp3"

    blob = audio_bucket.get_blob(track.origFilePath)
    input_file = blob.generate_signed_url(timedelta(hours=1))

    filters, taps = _get_ffmpeg_filters_and_taps(track_audio_ads)
    concat_filter = "{}concat=n={}:v=0:a=1[outaudio]".format("".join(taps), len(taps))
    filters.append(concat_filter)
    filter_string = ";".join(filters)

    input_files = [input_file]
    for ad in track_audio_ads:
        blob = audio_bucket.get_blob(ad.audioAd.get_file_path())
        filename = blob.generate_signed_url(timedelta(hours=1))
        input_files.append(filename)

    cmd_args = ["ffmpeg", "-nostats", "-y", "-hide_banner", "-v", "quiet"]
    for f in input_files:
        cmd_args += ["-i", f]
    cmd_args += ["-filter_complex", filter_string]
    cmd_args += ["-map", "[outaudio]", stitched_file_name]

    try:
        subprocess.run(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        duration = get_audio_length(stitched_file_name)
        duration_millis = duration * 1000
    except subprocess.CalledProcessError as e:
        logger.error(
            "Unable to stitch files",
            exc_info=True,
            returnCode=e.returncode,
            stderr=e.stderr,
            stdout=e.stdout,
            command=" ".join(cmd_args),
        )
        raise

    return stitched_file_name, duration_millis


def _get_hash(track: models.AudioTrack, track_audio_ads: List[models.AudioTrackAd]):
    """
    Generate a unique hash based on given track and ads.

    This is a reproduceable hash. Passing the same track and
    same set of ads in the same order generates the same hash.

    :param track: Original track
    :param track_audio_ads: List of AudioTrackAd objects
    :return: unique hash string
    """
    stitching_info = {
        "track_uid": track.uid,
        "durationMillis": track.durationMillis,
        "origFilePath": track.origFilePath,
    }

    ads_info = []
    for track_ad in track_audio_ads:
        audio_ad: models.AudioAd = track_ad.audioAd
        if audio_ad.adSourceId == models.AdServiceSource.THIRD_PARTY:
            raise ValueError("Only static ads are supported for stitching")

        ads_info.append(
            {
                "id": track_ad.audioAdId,
                "index": track_ad.index,
                "markInMillis": track_ad.markInMillis,
                "uploadId": audio_ad.uploadId,
            }
        )

    stitching_info["ads"] = ads_info
    encoded = json.dumps(stitching_info, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.md5(encoded).hexdigest()


def _upload_stitched_mp3_file(local_filename: str, remote_file_basename: str):
    if not os.path.isfile(local_filename):
        raise ValueError(f"File not found: {local_filename}")

    blob = audio_bucket.blob(remote_file_basename)
    blob.upload_from_filename(local_filename, predefined_acl="publicRead", content_type="audio/mpeg")
    return blob.name


def _get_filename_from_hash(hash_string: str, ext: str = "mp3"):
    return f"v1-stitched/{hash_string}.{ext}"


def get_track_url_suffix(track: models.AudioTrack, track_audio_ads: List[models.AudioTrackAd]):
    if not track_audio_ads:
        raise ValueError("ads can not be empty")

    hash_string = _get_hash(track, track_audio_ads)
    remote_filename = _get_filename_from_hash(f"{track.id or ''}-{track.uid}-{hash_string}")
    if audio_bucket.get_blob(remote_filename):
        return remote_filename

    stitched_filename, duration_millis = _stitch_files_without_encoding(track, track_audio_ads)
    _upload_stitched_mp3_file(stitched_filename, remote_filename)
    os.remove(stitched_filename)
    return remote_filename


if __name__ == "__main__":

    def local_file_test():
        _uid = "someuid"
        track_file_path = "/Users/vinod/projects/data/audio/fiction_story_000.mp3"
        ad_segments = [
            {"markInMillis": 12000, "filepath": "/Users/vinod/projects/data/audio/short-1.mp3"},
            {"markInMillis": 22000, "filepath": "/Users/vinod/projects/data/audio/short-2.mp3"},
        ]
        concatenated_file = _concat_files(_uid, track_file_path, ad_segments)
        print("local testing output file", concatenated_file)

    track_uid = "Iue7KcAD7iKl7XVh"

    with db_session_maker.context_session() as sess:
        track = sess.query(models.AudioTrack).get(track_uid)
        print("Track", track.id, track.uid, track.name, track.durationMillis)

        def without_encoding():
            track_ads = (
                sess.query(models.AudioTrackAd)
                .filter(models.AudioTrackAd.audioTrackUid == "Iue7KcAD7iKl7XVh")
                .order_by(models.AudioTrackAd.index.asc())
                .all()
            )
            out_filename = _stitch_files_without_encoding(track, track_ads)
            print("stitched output file (with concat)", out_filename)

        def with_encoding():
            track_ads = (
                sess.query(models.AudioTrackAd)
                .filter(models.AudioTrackAd.audioTrackUid == track_uid)
                .order_by(models.AudioTrackAd.index.asc())
                .all()
            )
            out_filename = _stitch_files_with_encoding(track, track_ads)
            print("stitched output file (with encoding)", out_filename)

        start_time = datetime.utcnow().timestamp()
        with_encoding()
        print("Duration", datetime.utcnow().timestamp() - start_time)

        start_time = datetime.utcnow().timestamp()
        without_encoding()
        print("Duration", datetime.utcnow().timestamp() - start_time)