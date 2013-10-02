### MOS/Observation analysis scripts

Some scripts for scraping/reading/analyzing archived GFS/NAM MOS output, and some stubs for obtaining observations as well.

The big utility script is `util.mos` which has the methods `get_NAM(station, years)` and `get_GFS(station, years)`. Supplying these routines a station such as `KSYR` and a list of years such as `range(2009, 20012)` will, in this case, download and uncompress all the available NAM or GFS MOS products, which will be archived in 

    ./
     /data_arch/
     -------->/{STATION}/
     -------->|-------->/GFS
     -------->|-------->/NAM

The other scripts provide some functionality for parsing the MOS blocks (incomplete at this point, but enough to extract the important information), aligning them in [pandas][] `DataFrame`s, and performing some really basic machine learning tasks.

#### Dependencies

- [pandas][]
- numpy
- matplotlib (optional; default library for plotting utilities)
- iPython (for notebooks, parallelization of downloading/analysis)

#### Documentation

Please refer to the [numpydoc](https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt) standard for documenting any code you write.

[pandas]: http://pandas.pydata.org/