
import sys
import time
import subprocess
import shutil

from multiprocessing import Process
from deepdiff import DeepDiff
from avro.datafile import DataFileReader
from avro.io import DatumReader
from pathlib import Path

REFERENCE_CLIENT = 'ftrcalcsteststandard'
GCP_BUCKET_LOCATION = "gs://md_stag_graphdata/"
REFERENCE_DIR = "reference"
CHANGED_DIR = 'changed'
DEBUG = False
DELETE_CACHED_FILES = True
COMPARISONS = 0
FAILED_COMPARISONS = 0


def run_subprocess(cmd, error=False):
    if error:
        popen = subprocess.Popen(cmd, stderr=subprocess.PIPE, universal_newlines=True)
        for stderr_line in iter(popen.stderr.readline, ""):
            yield stderr_line
        popen.stderr.close()
    else:
        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for stdout_line in iter(popen.stdout.readline, ""):
            yield stdout_line
        popen.stdout.close()

    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


def get_client_test_avros(client):
    test_avros = {}
    client_url = f"{GCP_BUCKET_LOCATION}{client}/"
    cmd = ["gsutil", "ls", client_url]
    test_urls = run_subprocess(cmd)
    count = 0
    for avro in test_urls:
        avro_file = avro.replace(client_url, '').strip()
        count += 1
        if DEBUG:
            print(avro_file, file=sys.stderr)
        else:
            if count % 1000 == 0:
                print(end='.', file=sys.stderr)
        # noinspection PyBroadException
        try:
            test_num = int(avro_file[0:avro_file.find('-')])
        except:
            test_num = 0
            pass # ignore non-numeric (job) buckets or objects

        if test_num not in test_avros:
            test_avros[test_num] = [avro_file]
        else:
            test_avros[test_num].append(avro_file)
    return test_avros


def compare_clients(client1, client2, test_id):
    test_avros1 = get_client_test_avros(client1)
    test_avros2 = get_client_test_avros(client2)
    diff = DeepDiff(test_avros1, test_avros2, ignore_order=True)
    if len(diff.to_dict().items()) == 0:
        if DEBUG and test_id:
            print(f"{len(test_avros1)} TESTS WITH IDENTICAL AVRO FILENAMES", file=sys.stderr)
        return test_avros1
    else:
        return diff


def read_avro_from_disk(avro_file):
    try:
        rows = []
        reader = DataFileReader(open(avro_file, "rb"), DatumReader())
        for record in reader:
            rows.append(record)
        reader.close()
    except BaseException as e:
        print(f"Problem reading avro file: {e}")
    return rows


def download_avro_files(client, temp_dir):
    avros_dir = f"{GCP_BUCKET_LOCATION}{client}"
    print(f"\nDownloading avros from {avros_dir}", file=sys.stderr)
    ref_cmd = ["gsutil", "-m", "cp", "-r", avros_dir, temp_dir]
    ref_results = run_subprocess(ref_cmd, error=True)
    count = 0
    for ref_line in ref_results:
        count += 1
        if DEBUG:
            print(ref_line.strip(), file=sys.stderr)
        else:
            if count % 1000 == 0:
                print(end='.', file=sys.stderr)


def compare_avro_files(reference_client, changed_client, changed_avros, test_id):
    if DELETE_CACHED_FILES or not (Path(REFERENCE_DIR).exists() and Path(CHANGED_DIR).exists()):
        Path(REFERENCE_DIR).mkdir(parents=True, exist_ok=True)
        Path(CHANGED_DIR).mkdir(parents=True, exist_ok=True)
        download_avro_files(reference_client, REFERENCE_DIR)
        download_avro_files(changed_client, CHANGED_DIR)
    threads = []
    for test in changed_avros:
        if (test_id and test_id == test) or not test_id:
            print(f"\nComparing {len(changed_avros[test])} avro files for test ID: {test}", file=sys.stderr)
            for avro in changed_avros[test]:
                ref_file = f"{REFERENCE_DIR}/{reference_client}/{avro}"
                changed_file = f"{CHANGED_DIR}/{changed_client}/{avro}"
                new_thread = Process(target=deep_difference_avro, args=(avro, ref_file, changed_file))
                if DEBUG:
                    print(f"Starting thread: {new_thread.name}", file=sys.stderr)
                threads.append(new_thread)
                new_thread.start()

    for t in threads:
        t.join()
        sys.stdout.flush()
        if DEBUG:
            print(f"Thread {t.name} has finished", file=sys.stderr)
        sys.stdout.flush()


def deep_difference_avro(avro, ref_file, test_file):
    global COMPARISONS, FAILED_COMPARISONS
    COMPARISONS += 1
    ref_avro = read_avro_from_disk(ref_file)
    test_avro = read_avro_from_disk(test_file)
    compare_result = DeepDiff(ref_avro, test_avro)
    if len(compare_result.to_dict().items()) == 0:
        if DEBUG:
            print(f"  {avro} is IDENTICAL", file=sys.stderr)
        else:
            print(end='.', file=sys.stderr)
    else:
        print(f"There are DIFFERENCES in {avro}:\n\n{compare_result.to_json(indent=2)}")
        FAILED_COMPARISONS += 1
        COMPARISONS += 1


def regression_test(reference_client, test_client, test_id=None):
    print(f"\nComparing avros from \"{test_client}\" to reference avros in \"{reference_client}\":", file=sys.stderr)
    compare_result = compare_clients(reference_client, test_client, test_id)
    if type(compare_result) == DeepDiff:
        print(compare_result.to_json(indent=2))
        print('\nAVRO FILENAMES ARE NOT IDENTICAL', file=sys.stderr)
        exit(1)

    compare_avro_files(reference_client, test_client, compare_result, test_id)


start = time.perf_counter()


#  @atexit.register  Doesn't work well with gsutil -m
def print_timer():
    time_elapsed = time.perf_counter() - start
    sys.stdout.flush()
    print(f"\n\nExecution took: {time_elapsed} seconds.\n\n", file=sys.stderr)
    sys.stdout.flush()


if __name__ == '__main__':

    if DELETE_CACHED_FILES:
        try:
            shutil.rmtree(REFERENCE_DIR)
            shutil.rmtree(CHANGED_DIR)
        except FileNotFoundError:
            pass  # ignore errors

    num_args = len(sys.argv)
    if num_args < 2 or num_args > 3:
        print("\nUSAGE:  regression.py feature_client [test_id]\n")
        exit(1)
    test_id = None
    if num_args == 3:
        test_id = int(sys.argv[2])
    regression_test(REFERENCE_CLIENT, sys.argv[1], test_id)

    if FAILED_COMPARISONS:
        print(f"{FAILED_COMPARISONS} FILES WERE DIFFERENT")
    else:
        print("ALL FILES WERE IDENTICAL")

    if DELETE_CACHED_FILES:
        shutil.rmtree(REFERENCE_DIR)
        shutil.rmtree(CHANGED_DIR)

    print_timer()
