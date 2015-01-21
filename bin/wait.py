#!/usr/bin/env python

import sys
import subprocess

def main():
    while True:
        p = subprocess.Popen(sys.argv[1:])
        p.communicate()

if __name__ == '__main__':
    main()
