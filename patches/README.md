Patches
=======

Patches for the liburkel that are either local to the library or are not yet
merged to the upstream.

To generate patch files you can use git diff from the branch where you have
written changes:
  `git diff --src-prefix=a/deps/liburkel/ --dst-prefix=b/deps/liburkel/ master > patch-file`
  
Note: patches need to be listed in the `.patches` file.
