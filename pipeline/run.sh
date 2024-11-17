#!/bin/bash

python logger.py
python load.py &
python transform.py &

yacron -c yacron.conf
