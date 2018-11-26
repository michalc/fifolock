import asyncio
import collections
import unittest

from fifolock import FifoLock


TaskState = collections.namedtuple('TaskState', ['started', 'done', 'task'])


def create_lock_tasks(lock, *modes):
    async def access(mode, started, done):
        async with lock(mode):
            started.set_result(None)
            await done

    def task(mode):
        started = asyncio.Future()
        done = asyncio.Future()
        task = asyncio.create_task(access(mode, started, done))
        return TaskState(started=started, done=done, task=task)

    return [task(mode) for mode in modes]


def has_started(task_states):
    return [state.started.done() for state in task_states]


def async_test(func):
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


class Read(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Write]


class Write(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Read] and not holds[Write]


class SemaphoreBase(asyncio.Future):
    @classmethod
    def is_compatible(cls, holds):
        return holds[cls] < cls.size


class TestFifoLock(unittest.TestCase):

    @async_test
    async def test_read_write_lock(self):
        lock = FifoLock()
        task_states = create_lock_tasks(lock, Write, Read, Read, Write, Read, Read)

        await asyncio.sleep(0)
        # Ensure only the first write has started...
        self.assertEqual(all(has_started(task_states)[0:1]), True)
        self.assertEqual(any(has_started(task_states)[1:]), False)

        task_states[0].done.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        # ... then only the next reads start...
        self.assertEqual(all(has_started(task_states)[1:3]), True)
        self.assertEqual(any(has_started(task_states)[3:]), False)

        task_states[2].done.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        # ... tasks finishing out of order doesn't change things...
        self.assertEqual(any(has_started(task_states)[3:]), False)

        task_states[1].done.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        # ... a queued write starts only after all previous access finished...
        self.assertEqual(all(has_started(task_states)[3:4]), True)
        self.assertEqual(any(has_started(task_states)[4:]), False)

        task_states[4].task.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        # ... cancelling a task doesn't change things...
        self.assertEqual(all(has_started(task_states)[3:4]), True)
        self.assertEqual(any(has_started(task_states)[4:]), False)

        task_states[3].done.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        # ... and doesn't stop later tasks from running
        self.assertEqual(all(has_started(task_states)[5:6]), True)

        concurrently_added_task = create_lock_tasks(lock, Read)
        await asyncio.sleep(0)
        # ... and a read requested during a read can proceed
        self.assertEqual(all(has_started(concurrently_added_task)), True)

        task_states[5].done.set_result(None)
        concurrently_added_task[0].done.set_result(None)

    @async_test
    async def test_semaphore(self):
        lock = FifoLock()
        Semaphore = type('Semaphore', (SemaphoreBase, ), {'size': 2})

        task_states = create_lock_tasks(lock, Semaphore, Semaphore, Semaphore)

        await asyncio.sleep(0)
        # Ensure only the first two have started...
        self.assertEqual(all(has_started(task_states)[0:2]), True)
        self.assertEqual(any(has_started(task_states)[2:]), False)

        # ... and one finishing allows the final to proceed
        task_states[1].done.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(all(has_started(task_states)[2:2]), True)

        task_states[0].done.set_result(None)
        task_states[2].done.set_result(None)
