"""
A demonstration of how to use `bream`. This example sets up a simple data source
that writes small files (with random names) of numbers to a directory, and a bream Stream
that streams these lists of numbers into a batch function that coomputes and writes
basic stats.

The source can be started with
>> python filesbytimestamp.py startsource <directory to put number files>

The stream can be started in a separate process with
>> python filesbytimestamp.py startstream <input dir, same as where to put files> <output file> <stream tracking dir>  # noqa: E501
"""

from __future__ import annotations

from pathlib import Path
from random import random, randint, choices
from functools import partial
from sys import argv
from time import sleep
from statistics import mean, stdev
from contextlib import suppress

from bream.core import Source, BatchRequest, Batch, Stream


def file_creation_loop(testdir: Path) -> None:
    """An example data source.
    
    Every now and then, adds a randomly-named file to `test_dir` that contains
    some numbers.
    """
    testdir.mkdir(parents=True, exist_ok=True)

    max_seconds_between_file_creation = 24

    while True:
        sleep_time = random()*max_seconds_between_file_creation
        sleep(sleep_time)
        fname = "".join(choices("abcdefghijklmnopqrstuvwxyz0123456789", k=10))
        number_of_numbers = randint(1, 6)
        nums = [f"{randint(0, 50)}\n" for _ in range(number_of_numbers)]
        print(f"Writing file: {fname}...")
        with (testdir / str(fname)).open("w") as f:
            f.writelines(nums)

        # cleanup: keep only latest 250 files
        ps = sorted(testdir.glob("*"), key=(lambda p: p.stat().st_ctime))
        for p in ps[:-500:]:
            p.unlink()


class NumbersFromFilesByCreationTimestampSource(Source):
    """Bream wrapper for a directory of files containing numbers. The data will
    be emitted in order of file creation timestamp. The file creation timestamp in
    POSIX timestamp form is used as the stream "offset".
    
    For this example, it will be be pointed at a directory populated by
    `file_creation_loop`.
    """

    def __init__(self, sourcename: str, filedir: Path, num_files_per_batch: int) -> None:
        """Initialize the bream data source.
        
        Args:
            sourcename: the name of the source - used by bream to identify the source
            filedir: path to the dir containing files of numbers
            num_files_per_batch: the maximum number of files that should be read in each batch
        """
        # the name of the source - used by bream to identify the source:
        self.name = sourcename
        self._filedir = filedir
        self._num_files_per_batch = num_files_per_batch

    def _get_timestamp_to_file_map(self) -> dict[int, Path]:
        """Get a map of file-creation timestamps to file path for each file in the dir."""
        files = list(self._filedir.glob("*"))
        res: dict[int, Path] = {}
        for f in files:
            with suppress(FileNotFoundError):  # avoid cleanup race-condition of file_creation_loop
                res[int(f.stat().st_ctime*1000)] = f
        return res

    def read(self, br: BatchRequest) -> Batch | None:
        """Read a batch of number-lists from the directory.

        The data will be read to a map with a key-value pair for each file read in this batch:
            {<filename>: [<numbers>, ...], ...}
        
        Args:
            br: The batch request. This is a bream construct that is
                part of the bream source protocol that looks like:
                BreamRequest(read_from_after: int | None, read_to: int | None) where:
                    `read_from_after` is an offset such that we should start
                    from the next available one (or None if we should start from the 
                    beginning)
                    `read_to` is the offset we should read to (or None if the source
                    gets to choose where to read to).

        Returns:
            batch: The batch we return. `Batch` is another bream construct that is part
                of the bream source protocol and looks like:
                Batch(data: Any, read_to: int) where:
                    `data` is the batch data
                    `read_to` is the offset we actually read to.
                If there is no data available, this should be None.
        """

        # if there are no files, we can't read any data:
        if not (timestamp_to_file_map := self._get_timestamp_to_file_map()):
            return None
    
        # get the timestamps in sorted order
        timestamps = sorted(timestamp_to_file_map)

        # read from the next timestamp if a 'read_from_after' is given, otherwise start at beginnig
        read_from_idx = (
            (timestamps.index(br.read_from_after) + 1) if br.read_from_after is not None else 0
        )

        # if there are no more files to read, we won't ready any data
        if read_from_idx == len(timestamps):
            return None
        
        if br.read_to is not None:
            # if told where to read to, respect it
            read_to_idx = timestamps.index(br.read_to)
        else:
            # otherwise read as many files as configured to
            read_to_idx = min(read_from_idx + self._num_files_per_batch - 1, len(timestamps) - 1)

        # get the actual timestamps of files we should read
        timestamps_of_files_to_read =  timestamps[read_from_idx:read_to_idx+1]
            
        # get the file paths we need to read
        files_to_read = [timestamp_to_file_map[ts] for ts in timestamps_of_files_to_read]

        # get the data from the files
        data: dict[str, list[int]] = {}
        for path in files_to_read:
            with path.open("r") as f:
                cleaned = [x for x in f.read().strip().split("\n") if x]
            nums = [int(x) for x in cleaned]
            data[path.name] = nums

        return Batch(data=data, read_to=timestamps_of_files_to_read[-1])
    

def write_stats(batch: Batch, output_file: Path) -> None:
    """Example batch function that takes data from NumbersFromFilesByCreationTimestampSource.
    
    This will be given to the bream `Stream` object as the batch processing function and
    computes some basic stats for each list of numbers read from the files.

    Args:
        batches: A bream construct that looks like
            Batches(batches: dict[str, Batch | None] = {<source name>: Batch(...), ...}) where
            `batches` is a map of source name to Batch object. In this example there will be only
            one key-value pair, but in general there will be one for each source of the stream.
        output_file: Path to file we should write the output stats too.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # get and report batch of numbers:
    assert batch is not None
    print(f"Seen batch: {batch}")
    nums: dict[str, list[int]] = batch.data
    # as per the source, the data is a map of the form {<filename>: [<numbers>, ...], ...}

    # make the function flaky:
    if random() < 0.1:
        raise RuntimeError(f"raising error on batch: {batch}")

    # process the batch and write output
    with output_file.open("a") as f:
        for fname, numlist in nums.items():

            # compute stats:
            mean_ = 0 if len(numlist) < 1 else mean(numlist)
            stdev_ = 0 if len(numlist) < 2 else stdev(numlist)
            stats = [len(numlist), mean_, stdev_]

            # write stats:
            stats_writable = (
                f"{fname}: {numlist} -> "
                f"(count={stats[0]}, mean={stats[1]}, stdev={stats[2]})\n"
            )
            f.write(stats_writable)


def start_stream(input_dir: Path, output_file: Path, stream_dir: Path) -> None:
    """Start the stream for the example..
    
    Args:
        input_dir: path to dir of files to point NumbersFromFilesByCreationTimestampSource at
        output_file: path to file where batch function should write stats to
        stream_dir: path to dir where stream will track its progress
    """

    # define the source:
    source_name = "some_numbers"
    source = NumbersFromFilesByCreationTimestampSource(
        source_name, input_dir, num_files_per_batch=3
    )

    # define the batch function:
    batch_func = partial(write_stats, output_file=output_file)

    # define and start the stream: batch function is flaky so wrap it in a restart-loop:
    while True:
        stream = Stream(source, stream_dir)
        # start stream in background thread, and don't read batches more often than 30 secs
        stream.start(batch_func, min_batch_seconds=30)
        stream.wait()  # block until stream dies
        if stream.status.error:  # stream died which means an error happened, check status
            print(f"error raised: {repr(stream.status.error)}")

if __name__ == "__main__":
    if argv[1] == "startsource":
        inputdir = Path(argv[2])
        file_creation_loop(inputdir)
    elif argv[1] == "startstream":
        inputdir = Path(argv[2])
        outputdir = Path(argv[3])
        streamdir = Path(argv[4])
        start_stream(inputdir, outputdir, streamdir)
    else:
        raise ValueError
