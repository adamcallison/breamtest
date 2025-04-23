"""Simple dummy demo."""

from __future__ import annotations

from functools import partial
from pathlib import Path
from sys import argv

from bream.core import Batch, Batches, BatchRequest, Source, Stream


class DemoError(Exception):
    """Raised during demo."""


class DummySource(Source):
    """A fake source that just produces some simple dicts."""

    def __init__(self, name: str, num_dicts: int, dicts_per_batch: int) -> None:
        """Initialize dummy source."""
        self.name = name
        self._dicts = [{"id": j, "data": f"{name}_data_{j}"} for j in range(num_dicts)]
        self._dicts_per_batch = dicts_per_batch

    def read(self, batch_request: BatchRequest) -> Batch | None:
        """Read data."""
        read_from = (
            (batch_request.read_from_after + 1) if batch_request.read_from_after is not None else 0
        )

        if batch_request.read_to is not None:
            read_to = batch_request.read_to
        else:
            num_dicts = len(self._dicts)
            if read_from == num_dicts:
                return None
            read_to = min(num_dicts - 1, read_from + self._dicts_per_batch - 1)

        data = self._dicts[read_from : read_to + 1]
        return Batch(data=data, read_to=read_to)


def _combine_batches(batch1: Batch | None, batch2: Batch | None) -> list[dict]:
    dicts: list[dict]
    if batch1 is None:
        assert batch2 is not None  # noqa: S101
        dicts = batch2.data
    elif batch2 is None:
        assert batch1 is not None  # noqa: S101
        dicts = batch1.data
    else:
        dicts = batch1.data + batch2.data
    return dicts


def batch_func(batches: Batches, *, raise_error: bool = False) -> None:
    """Process a dummy batch."""
    raise_error_if_gt_than = 4

    demo_batch1, demo_batch2 = batches["demo_source_1"], batches["demo_source_2"]
    demo_batch2 = batches["demo_source_2"]
    dicts = _combine_batches(demo_batch1, demo_batch2)

    print("==== batch_func_called ====")
    print("dicts:")
    for dict_ in dicts:
        print(dict_)
    print("============================")
    if raise_error and demo_batch1 and demo_batch1.read_to > raise_error_if_gt_than:
        msg = f"read_to was {demo_batch1.read_to}"
        raise DemoError(msg)


def main(demo_path: Path) -> None:
    """Run the demo."""
    # define demo sources:
    source1 = DummySource("demo_source_1", 100, 3)
    source2 = DummySource("demo_source_2", 21, 2)

    # here's where we'll track stream progress
    stream_path = demo_path / "stream"

    # configure the stream
    stream = Stream(sources=[source1, source2], stream_path=stream_path)
    # run the version that raises an error after a bit first:
    stream.start(partial(batch_func, raise_error=True), min_batch_seconds=0.5)
    stream.wait()
    print("Error raised:")
    print(repr(stream.status.error))

    # configure the stream again
    stream = Stream(sources=[source1, source2], stream_path=stream_path)
    # now run the version that doesn't raise an error, we'll see
    # that the batch gets rerun
    stream.start(batch_func, min_batch_seconds=0.5)
    stream.wait()


if __name__ == "__main__":
    demo_path = Path(argv[1])
    main(demo_path)
