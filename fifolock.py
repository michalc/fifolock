from asyncio import (
    CancelledError as _CancelledError,
)
from collections import (
    defaultdict as _defaultdict,
    deque as _deque,
)


class FifoLock():

    def __init__(self):
        self._waiters = _deque()
        self._holds = _defaultdict(int)

    def __call__(self, lock_mode_type):
        return _FifoLockContextManager(self._waiters, self._holds, lock_mode_type)


class _FifoLockContextManager():

    def __init__(self, waiters, holds, lock_mode_type):
        self._waiters = waiters
        self._holds = holds
        self._lock_mode_type = lock_mode_type

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

    async def __aenter__(self):
        lock_mode = self._lock_mode_type()
        self._waiters.append(lock_mode)
        self._maybe_acquire()
        try:
            await lock_mode
        except _CancelledError:
            # The waiter could have been resolved, but the task then cancelled
            if lock_mode.done() and not lock_mode.cancelled():
                self._holds[type(lock_mode)] -= 1
                self._maybe_acquire()
            raise

    async def __aexit__(self, _, __, ___):
        self._holds[self._lock_mode_type] -= 1
        self._maybe_acquire()
