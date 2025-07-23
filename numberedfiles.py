# note: won't work exactly after refactoring bream

from __future__ import annotations

from random import random, randint
from time import sleep
from pathlib import Path
from sys import argv

from bream.core import Source, BatchRequest, Batch, Batches, Stream
from functools import partial

def file_creation_loop(testdir: Path) -> None:
    testdir.mkdir(parents=True, exist_ok=True)
    curr = max([int(p.name) for p in testdir.glob("*")] or [-1])
    while True:
        sleep_time = random()*10
        sleep(sleep_time)
        curr += 1
        num_nums = randint(1, 10)
        nums = [f"{randint(0, 100)}\n" for _ in range(num_nums)]
        print(f"Writing file: {curr}...")
        with (testdir / str(curr)).open("w") as f:
            f.writelines(nums)

        # cleanup
        files_to_delete = sorted([int(p.name) for p in testdir.glob("*")])[:-250]
        for ftd in files_to_delete:
            (testdir / str(ftd)).unlink()


class NumberFileSource(Source):
    def __init__(self, sourcename: str, filedir: Path) -> None:
        self.name = sourcename
        self._filedir = filedir

    def _determine_file_to_read_from_batch_request(self, br: BatchRequest) -> int | None:
        read_from = (br.read_from_after + 1) if br.read_from_after is not None else 0

        if br.read_to is not None:
            read_to = br.read_to
        else:
            latest_file = max([int(p.name) for p in self._filedir.glob("*")] or [-1])
            if read_from == latest_file + 1:
                return None
            read_to = min(latest_file, read_from)
        return read_to

    def read(self, br: BatchRequest) -> Batch | None:
        file_to_read = self._determine_file_to_read_from_batch_request(br)
        if file_to_read is None:
            return None

        with (self._filedir / str(file_to_read)).open("r") as f:
            cleaned = [x for x in f.read().strip().split("\n") if x]
            nums = [int(x) for x in cleaned]
        return Batch(data=nums, read_to=file_to_read)


def square_numbers(batches: Batches, source_name: str, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    batch = batches[source_name]
    assert batch is not None
    print(f"Seen batch: {batch}")
    nums: list[int] = batch.data
    squares = [x**2 for x in nums]
    squares_writable = f"{batch.read_to}: {nums} -> {squares}\n"
    if random() < 0.1:
        raise RuntimeError(f"raising error on batch: {batch}")
    with output_file.open("a") as f:
        f.write(squares_writable)


def start_stream(input_dir: Path, output_file: Path, stream_dir: Path) -> None:
    source = NumberFileSource("some_numbers", input_dir)
    batch_func = partial(square_numbers, source_name="some_numbers", output_file=output_file)
    while True:
        stream = Stream([source], stream_dir)
        stream.start(batch_func, min_batch_seconds=3)
        stream.wait()
        if stream.status.error:
            print(f"error raised: {repr(stream.status.error)}")

if __name__ == "__main__":
    if argv[1] == "startsource":
        testdir = Path(argv[2])
        file_creation_loop(testdir)
    elif argv[1] == "startstream":
        inputdir = Path(argv[2])
        outputdir = Path(argv[3])
        streamdir = Path(argv[4])
        start_stream(inputdir, outputdir, streamdir)
    else:
        raise ValueError
