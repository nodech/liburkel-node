#!/bin/sh

echo "Applying patches to deps..."
for f in `cat patches/.patches`
do
  echo "Applying $f to deps..."
  patch -p1 -N -s < patches/$f
done
