#!/bin/sh

echo "Un-Applying patches to deps..."
for f in `cat patches/.patches`
do
  echo "Un-Applying $f to deps..."
  patch -p1 -N -s -R < patches/$f
done
