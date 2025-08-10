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
from datetime import datetime, timezone
from itertools import islice, takewhile
from typing import cast

from bream.core import Source, BatchRequest, Batch, Stream, StreamOptions


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

    def _get_sorted_timestamp_to_filename_map(
            self, *, later_than: int | None = None,
        ) -> dict[int, str]:
        """Get a map of file-creation timestamps to file path for each file in the dir.
        
        If `later_than` is not None, files with this or earlier timestamp will not be
        included.
        """
        files = list(self._filedir.glob("*"))
        timestamp_to_filename_map: dict[int, str] = {}
        for f in files:
            with suppress(FileNotFoundError):  # avoid cleanup race-condition of file_creation_loop
                timestamp_to_filename_map[int(f.stat().st_ctime*1000)] = f.name
        if later_than is None:
            later_than = cast(int, -float("inf"))
        return {
            k: v for k, v in sorted(timestamp_to_filename_map.items()) if k > later_than
        }
    
    def _read_file(self, filename: str) -> list[int]:
        with (self._filedir / filename).open("r") as f:
            cleaned = [x for x in f.read().strip().split("\n") if x]
        return [int(x) for x in cleaned]

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

        # some type narrowing
        br_read_to = cast(int, br.read_to)
        br_read_from_after = cast(int, br.read_from_after)

        # find what files are unread
        unread_timestamp_to_fname = self._get_sorted_timestamp_to_filename_map(
            later_than=br_read_from_after
        )

        # if there are no files to read, we don't return a batch
        if not unread_timestamp_to_fname:
            return None

        # find out which of the unread files we should read for this batch
        iterfrom = (
            # respect BatchRequest.read_to if it is not None
            takewhile((lambda x: x[0] <= br_read_to), unread_timestamp_to_fname.items())
            if br.read_to is not None
            # otherwise take as many files as we're configured to
            else islice(unread_timestamp_to_fname.items(), self._num_files_per_batch)
        )
        timestamp_to_filename_to_read = {k: v for k, v in iterfrom}

        # get the data from the files
        data = {fn: self._read_file(fn) for fn in timestamp_to_filename_to_read.values()}

        # return a batch
        return Batch(data=data, read_to=max(timestamp_to_filename_to_read.keys()))


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
    print(f"Seen batch: {batch} at {datetime.now(tz=timezone.utc).isoformat()}")
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

    # define and start the stream:
    stream = Stream(
        source,
        stream_dir,
        stream_options=StreamOptions(max_retry_count=None, min_seconds_between_retries=10),
    )
    #start stream in background thread, and don't read batches more often than 3.6 secs
    stream.start(batch_func, min_batch_seconds=3.6)

    # batch function is flaky so let's monitor stream for errors
    error_count = 0
    while True:
        new_error_count = len(stream.status.errors)
        if new_error_count != error_count:
            error_count = new_error_count
            print(f"New error detected (total {error_count}): {repr(stream.status.errors[-1])}")
            sleep(0.25)


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
