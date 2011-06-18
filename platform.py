# coding: utf-8

from sys import platform

if platform == 'darwin':
    DIFF_TOOL = ['opendiff']
else:
    DIFF_TOOL = ['start', r'winmerge\WinMergeU.exe',]

def remote(parsed):
    if platform == 'darwin':
        return [
            'ssh'
        ]
    else:
        return  [
            'plink',
            '-batch',
            '-C',
            '-l',  parsed.username,
            '-pw', parsed.password,
        ]
