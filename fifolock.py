import asyncio
import collections
import contextlib


class FifoLock():

    def __init__(self):
        self._waiters = collections.deque()
        self._holds = collections.defaultdict(int)

    def _maybe_acquire(self):
        while self._waiters:

            if self._waiters[0].cancelled():
                self._waiters.popleft()

            elif self._waiters[0].is_compatible(self._holds):
                waiter = self._waiters.popleft()
                self._holds[type(waiter)] += 1
                waiter.set_result(None)

            else:
                break

    @contextlib.asynccontextmanager
    async def __call__(self, lock_mode_type):
        lock_mode = lock_mode_type()
        self._waiters.append(lock_mode)
        self._maybe_acquire()

        try:
            await lock_mode
        except asyncio.CancelledError:
            self._maybe_acquire()
            raise

        try:
            yield
        finally:
            self._holds[type(lock_mode)] -= 1
            self._maybe_acquire()
