import time
from functools import wraps
from memory_profiler import memory_usage
from typing import Iterator, Optional
import io


class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ""

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret) :]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return "".join(line)


def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        print(f"\n{fn.__name__}({fn_kwargs_str})")

        # Measure time and memory
        t = time.perf_counter()
        mem, retval = memory_usage(
            (fn, args, kwargs), retval=True, timeout=200, interval=1e-7
        )
        elapsed = time.perf_counter() - t
        print(f"Time   {elapsed:0.4}")
        print(f"Memory {max(mem) - min(mem)}")
        return retval

    return inner
