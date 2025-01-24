# Nurkel

[![Node.js](https://github.com/nodech/nurkel/actions/workflows/node.js.yml/badge.svg)](https://github.com/nodech/nurkel/actions/workflows/node.js.yml)

**Experimental implementation/bindings to the liburkel.**

Bindings to [liburkel](https://github.com/chjj/liburkel).
It uses [urkel](https://github.com/handshake-org/urkel) for the in memory version and
has wrapper around it that is compatible with the nurkel async API. NOTE: urkel wrapper
API does not have sync API.

Support of Node.js: >= 14.x

Unfortunately, for now, there are differencies with the urkel.
  - Transaction and Snapshot needs `open()` call before using and `close` after using it.
  - Every method (with exception of `rootHash`) is async by default.
  - Every method has `Sync` method companion.

### TODOs
  - Decide for which methods it is fine to use Sync methods instead of async. (benchmark)
