#!/bin/bash
CONFIG_DIR="$1"
source "$CONFIG_DIR/../../bin/activate"
python "$CONFIG_DIR/../generate.py" "$CONFIG_DIR"
cp -r $CONFIG_DIR/../datafiles /a/limn-public-data/mobile/datafiles
