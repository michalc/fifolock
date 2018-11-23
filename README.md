# fifolock

A flexible low-level tool to make synchronisation primitives in asyncio Python


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