'''utils.py - misc useful functions'''

import requests
import hashlib

def download_to_md5(url):
    '''streams input url into returned md5 hash string'''
    r = requests.get(url, stream=True)

    sig = hashlib.md5()

    # loop over lines in url response
    for line in r.iter_lines():
        sig.update(line)

    return sig.hexdigest()


def file_to_md5(filepath):
    '''streams input string into returned md5 hash string'''
    sig = hashlib.md5()
    with open(filepath, "rb") as f:
        # loop over bytes of file until there are no bytes left
        # reads 8192 bytes at a time - md5 has 128-byte digest blocks
        # and 8192 is 128*64
        # stackoverflow.com/questions/1131220/get-the-md5-hash-of-big-files-in-python
        while True:
            buf = f.read(8192)
            if not buf:
                break
            sig.update(buf)

    return sig.hexdigest()
    
    
