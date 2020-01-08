This is a toy project that demonstrates the use of Apache Arrow to manipulate data.

The input file is `xin` which has 2000 lines.
It is split from a larger SAM file using the [split command](https://stackoverflow.com/questions/2016894/how-to-split-a-large-text-file-into-smaller-files-with-equal-number-of-lines).

To run the code, first the Plasma Object Store has to be launched: `./launch_plasma_store.sh`

Then run `python3 put_sam_to_plasma.py`.
(To make programming a bit easier, this script crops out some optional SAM fields that are not in every line of the input SAM file.
As a side effect a file called `cropped_xin` will be created.)

The Record Batch can be retrieved using `./retrieve`, which is compiled from `retrieve.cc` using `compile.sh`.
