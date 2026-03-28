#!/usr/bin/env python3
import os
import socket
import hashlib
import binascii
import unittest
import requests
from urllib.parse import quote_plus
import time
import timeit
import logging
import base64
import subprocess
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(format='%(name)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class TestMiniKeyValue(unittest.TestCase):
  maxDiff = None
  
  def get_fresh_key(self):
    return b"http://localhost:3000/swag-" + binascii.hexlify(os.urandom(10))

  def test_getputdelete(self):
    key = self.get_fresh_key()

    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.get(key)
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.text, "onyou")

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

  def test_deleteworks(self):
    key = self.get_fresh_key()

    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

    r = requests.get(key)
    self.assertEqual(r.status_code, 404)

  def test_doubledelete(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

    r = requests.delete(key)
    self.assertNotEqual(r.status_code, 204)

  def test_doubleput(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.put(key, data="onyou")
    self.assertNotEqual(r.status_code, 201)

  def test_doubleputwdelete(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

  def test_10keys(self):
    keys = [self.get_fresh_key() for i in range(10)]

    for k in keys:
      r = requests.put(k, data=hashlib.md5(k).hexdigest())
      self.assertEqual(r.status_code, 201)

    for k in keys:
      r = requests.get(k)
      self.assertEqual(r.status_code, 200)
      self.assertEqual(r.text, hashlib.md5(k).hexdigest())

    for k in keys:
      r = requests.delete(k)
      self.assertEqual(r.status_code, 204)

  def test_range_request(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.get(key, headers={"Range": "bytes=2-5"})
    self.assertEqual(r.status_code, 206)
    self.assertEqual(r.text, "you")

  def test_nonexistent_key(self):
    key = self.get_fresh_key()
    r = requests.get(key)
    self.assertEqual(r.status_code, 404)

  def test_head_request(self):
    # head not exist
    key = self.get_fresh_key()
    r = requests.head(key, allow_redirects=True)
    self.assertEqual(r.status_code, 404)
    # no redirect, content length should be zero
    self.assertEqual(int(r.headers['content-length']), 0)

    # head exist
    key = self.get_fresh_key()
    data = "onyou"
    r = requests.put(key, data=data)
    self.assertEqual(r.status_code, 201)
    r = requests.head(key, allow_redirects=True)
    self.assertEqual(r.status_code, 200)
    # redirect, content length should be size of data
    self.assertEqual(int(r.headers['content-length']), len(data))

  def test_large_key(self):
    key = self.get_fresh_key()

    data = b"a"*(16*1024*1024)

    r = requests.put(key, data=data)
    self.assertEqual(r.status_code, 201)

    r = requests.get(key)
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.content, data)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

  def test_json_list(self):
    key = self.get_fresh_key()
    data = "eh"
    r = requests.put(key+b"1", data=data)
    self.assertEqual(r.status_code, 201)
    r = requests.put(key+b"2", data=data)
    self.assertEqual(r.status_code, 201)

    r = requests.get(key+b"?list")
    self.assertEqual(r.status_code, 200)
    bkey = key.decode('utf-8')
    bkey = "/"+bkey.split("/")[-1]
    self.assertEqual(r.json(), {"next": "", "keys": [bkey+"1", bkey+"2"]})

  def test_json_list_null(self):
    r = requests.get(self.get_fresh_key()+b"/DOES_NOT_EXIST?list")
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.json(), {"next": "", "keys": []})

  def test_json_list_limit(self):
    prefix = self.get_fresh_key()
    keys = []
    data = "0"
    limit = 10
    for i in range(limit+2):
      key = prefix+str(i).encode()
      r = requests.put(key, data=data)
      self.assertEqual(r.status_code, 201)
      keys.append("/"+key.decode().split("/")[-1])
    # leveldb is sorted alphabetically
    keys = sorted(keys)
    # should return first page
    r = requests.get(prefix+b"?list&limit="+str(limit).encode())
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.json(), {"next": keys[limit], "keys": keys[:limit]})
    start = quote_plus(r.json()["next"]).encode()
    # should return last page
    r = requests.get(prefix+b"?list&limit="+str(limit).encode()+b"&start="+start)
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.json(), {"next": "", "keys": keys[limit:]})

  def test_noemptykey(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="")
    self.assertEqual(r.status_code, 411)

  def test_content_hash(self):
    for i in range(100):
      key = self.get_fresh_key()
      r = requests.put(key, data=key)
      self.assertEqual(r.status_code, 201)

      r = requests.head(key, allow_redirects=False)
      self.assertEqual(r.headers['Content-Md5'], hashlib.md5(key).hexdigest())

  def _key_to_volume_path(self, key, volume_root):
    """Resolve the physical path of a key on a local volume root."""
    raw = key if isinstance(key, bytes) else key.encode()
    # strip http://localhost:3000 prefix to get the key path
    path = raw.split(b"localhost:3000", 1)[-1]
    import hashlib as hl, base64 as b64
    mkey = hl.md5(path).digest()
    b64key = b64.b64encode(path).decode()
    return os.path.join(volume_root, "%02x" % mkey[0], "%02x" % mkey[1], b64key)

  def _backdate_all_replicas(self, key, past):
    """Backdate the mtime of key on every replica volume to `past` (unix timestamp)."""
    import hashlib as hl, base64 as b64
    r = requests.head(key, allow_redirects=False)
    self.assertEqual(r.status_code, 302)
    kvols = r.headers.get("Key-Volumes", "")
    raw = key if isinstance(key, bytes) else key.encode()
    path = raw.split(b"localhost:3000", 1)[-1]
    mkey = hl.md5(path).digest()
    b64key = b64.b64encode(path).decode()
    kpath = "%02x/%02x/%s" % (mkey[0], mkey[1], b64key)
    for vol in kvols.split(","):
        # vol is e.g. "localhost:3005/sv06" — map port to /tmp/volumeN/
        host_port, sv = vol.split("/", 1)
        port = int(host_port.split(":")[1])
        file_path = "/tmp/volume%d/%s/%s" % (port - 3000, sv, kpath)
        if os.path.exists(file_path):
            os.utime(file_path, (past, past))

  def test_purge_expiry(self):
    """PUT a file, backdate its mtime on disk, trigger ?purge, verify it's gone."""
    key = self.get_fresh_key()
    r = requests.put(key, data=b"expire me")
    self.assertEqual(r.status_code, 201)

    # backdate mtime on all replicas to 10 days ago so expiry=5 will catch it
    past = time.time() - 10 * 86400
    self._backdate_all_replicas(key, past)

    # trigger purge via master (runs synchronously, 204 = done)
    r = requests.get("http://localhost:3000/?purge")
    self.assertEqual(r.status_code, 204)

    # file should now be gone
    r = requests.get(key)
    self.assertEqual(r.status_code, 404)

  def test_purge_concurrent_put(self):
    """Concurrent PUT during purge must not lose the new file."""
    # PUT an old file and backdate all its replicas before purge starts
    old_key = self.get_fresh_key()
    r = requests.put(old_key, data=b"old data")
    self.assertEqual(r.status_code, 201)

    past = time.time() - 10 * 86400
    self._backdate_all_replicas(old_key, past)

    # fire purge and a concurrent new PUT at the same time
    new_key = self.get_fresh_key()
    with ThreadPoolExecutor(max_workers=2) as ex:
      purge_future = ex.submit(requests.get, "http://localhost:3000/?purge")
      put_future   = ex.submit(requests.put, new_key, b"new data")

    self.assertEqual(purge_future.result().status_code, 204)
    self.assertEqual(put_future.result().status_code, 201)

    # old file must be gone
    r = requests.get(old_key)
    self.assertEqual(r.status_code, 404)

    # new file must still be alive
    r = requests.get(new_key)
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.content, b"new data")

if __name__ == '__main__':
  # wait for servers
  for port in range(3000,3006):
    print("check port %d" % port)
    while 1:
      try:
        s = socket.create_connection(("localhost", port), timeout=0.5)
        s.close()
        break
      except (ConnectionRefusedError, OSError):
        time.sleep(0.5)
        continue
      print("waiting for servers")
  
  unittest.main()

