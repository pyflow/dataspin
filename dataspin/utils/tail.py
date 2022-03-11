#!/usr/bin/env python


import os
import sys
import time

class TailError(Exception):
    def __init__(self, msg):
        self.message = msg
    def __str__(self):
        return self.message

class FileTail(object):
    def __init__(self, target_file):
        self.last_pos = 0
        self.check_file_validity(target_file)
        self.target_file = target_file

    def tail(self, seek_end=False):
        with open(self.target_file) as file_:
            file_.seek(self.last_pos, 0)
            if seek_end:
                lines = []
                file_.seek(0, 2)
                self.last_pos = file_.tell()
            else:
                lines = file_.readlines()
            while True:
                line = file_.readline()

                if not line:
                    file_.seek(self.last_pos, 0)
                    break
                else:
                    lines.append(line)
            current_pos = file_.tell()
            self.last_pos = current_pos
            return lines

    def check_file_validity(self, file_):
        ''' Check whether the a given file exists, readable and is a file '''
        if not os.access(file_, os.F_OK):
            raise TailError("File '%s' does not exist" % (file_))
        if not os.access(file_, os.R_OK):
            raise TailError("File '%s' not readable" % (file_))
        if os.path.isdir(file_):
            raise TailError("File '%s' is a directory" % (file_))