# fifolock

A flexible low-level tool to make synchronisation primitives in asyncio Python. As the name suggests, locks are granted strictly in the order requested: first-in-first-out.


## Installation

```bash
pip install fifolock
```


## Recipes

### Mutex (exclusive) lock

```python
import asyncio
from fifolock import FifoLock


class Mutex(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Mutex]


lock = FifoLock()

async def access():
    async with lock(Mutex):
        # access resource
```

### Read/write (shared/exclusive) lock

```python
import asyncio
from fifolock import FifoLock


class Read(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Write]

class Write(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Read] and not holds[Write]


lock = FifoLock()

async def read():
    async with lock(Read):
        # shared access

async def write():
    async with lock(Write):
        # exclusive access
```

### Semaphore

```python
import asyncio
from fifolock import FifoLock


class SemaphoreBase(asyncio.Future):
    @classmethod
    def is_compatible(cls, holds):
        return holds[cls] < cls.size


lock = FifoLock()
Semaphore = type('Semaphore', (SemaphoreBase, ), {'size': 3})

async def access():
    async with lock(Semaphore):
        # at most 3 concurrent accesses
```
