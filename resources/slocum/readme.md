# amlr-gliders slocum utilities

This folder contains utilities used to process slocum glider binary files. Specifically, it contains a shell script and associated binary files for converting and merging Slocum glider binary data files to ascii.

This folder is functionally a recreation of [kerfoot/slocum](https://github.com/kerfoot/slocum), so that we have a stable version of these tools that we manage:
- cac2lower.sh was copied directly from kerfoot/slocum.
- logging.sh was copied directly from kerfoot/slocum.
- processDbds-usamlr.sh is the same as processDbds.sh, except for a couple of path updates. 
- The current linux-bin binaries are from release 8_6 on https://datahost.webbresearch.com/

Note: This directory does not have tools to handle compressed dinkum data files. See [kerfoot/slocum](https://github.com/kerfoot/slocum) if these become needed.
