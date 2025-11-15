# TO DO

## Rechunking

Figure out why `calc_mvbs` is rechunking data. Sample. Note that if it doesn't
have to rechunk, things go pretty quickly.

```aiignore
Calculating MVBS from Sv file 250427WF-T172306.sv
Starting calculating MVBS for 250427WF-T172306.sv
Finished calculating MVBS for 250427WF-T172306.sv
Calculating MVBS from Sv file 250427WF-T172418.sv
Starting calculating MVBS for 250427WF-T172418.sv
Finished calculating MVBS for 250427WF-T172418.sv
Calculating MVBS from Sv file 250427WF-T172613.sv
/home/tkeffer/git/westernflyer/ek80/venv/lib/python3.12/site-packages/dask/array/core.py:4997: PerformanceWarning: Increasing number of chunks by factor of 16
  result = blockwise(
/home/tkeffer/git/westernflyer/ek80/venv/lib/python3.12/site-packages/dask/array/core.py:4997: PerformanceWarning: Increasing number of chunks by factor of 16
  result = blockwise(
/home/tkeffer/git/westernflyer/ek80/venv/lib/python3.12/site-packages/dask/array/core.py:4997: PerformanceWarning: Increasing number of chunks by factor of 16
  result = blockwise(
Starting calculating MVBS for 250427WF-T172613.sv
Finished calculating MVBS for 250427WF-T172613.sv
```
