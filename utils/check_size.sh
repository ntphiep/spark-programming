#!/bin/bash

# Kiểm tra xem đối tượng là file hay thư mục
if [ -f "$1" ]; then
    echo "Size of file $1 :"
    du -sh "$1"
elif [ -d "$1" ]; then
    echo "Size of folder $1 :"
    du -sh "$1"
else
    echo "$1 is not a file/folder"
fi
