# Spatially allocate gaps and estimate time lost to gaps

Files

 - GridActivity.py: Run this script first. It creates the tables that the others run.
 - CloseToLine.py: Estimates the fraction of activity, according to the raster method, that is within 111km of a straight line connecting the start and end of a gap.
 - GapsSpatiallyAllocated.py: Makes maps of activity for figure 1 and figure 2
 - PortTime.py: Estimates average voyage time of vessels on the high seas to estimate when they should return to port, on average.
 - TimeLostGapsbyCategory.py: Looks at the time lost to gaps at different distances and times. It justifies this sentence in the supplement "Over half the time lost to disabling events are in events that are shorter in distance than 100km, suggesting that our grid size of one degree may be roughly appropriate."
 - TotalTimeLosttoGaps.py: Builds the table that estimates the fraction of time lost to disabling events from different methods
 - VisualizeRasters.py: visualizes the probability rasters.
