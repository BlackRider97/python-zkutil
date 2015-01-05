python-zkutil
=============

Zookeeper tree import/export utility tool


requiremnets (specified requirements.txt)

1. $ pip install kazoo
2. $ pip install yaml

Usages:
------


- $ python zkutil.py <export/import> <source/distination-zk-path>  <output/input-yaml-file-path>

1. $ python zkutil.py export 127.0.0.1:2181 zk-backup.yml
2. $ python zkutil.py import 10.0.1.10:2181 zk-backup.yml




