Nurkel
======

**Experimental implementation/bindings to the liburkel.**

Bindings to [liburkel](https://github.com/chjj/liburkel).
API tries to be compatible with [urkel](https://github.com/handshake-org/urkel).

Support of Node.js: >= 12.x

Unfortunately, for now, there are differencies and some features are not yet implemented.
  - Transaction and Snapshot needs `open()` call before using and `close` after using it.
  - Every method (with exception of `rootHash`) is async by default.
  - Every method has `Sync` method companion.
  - There are no iterator interfaces.
  - No compaction yet.

### TODOs
There are couple of TODOs provided in the code, but here will be the list of higher level ones:
  - Decide for which methods it is fine to use Sync methods instead of async. (benchmark)
  - Compaction API (implement in liburkel).
  - Implement Tree Iterator.
  - Implement Transaction Iterator.
  - Virtual Transaction ?
