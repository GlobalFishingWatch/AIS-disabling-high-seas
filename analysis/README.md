# Analysis scripts

**figs_compare_loitering_and_gaps.py**: This script investigates whether AIS disabling events are closer to loitering activity than to fishing activity.

**figs_disabling_case_study.py**: This script produces Figure 4 from the main paper, demonstrating two case study examples of disabling events.

**figs_gap_stats.py**: This script calculates summary statistics of AIS disabling events, including by flag state and geartype.

**figs_justify_12_hour_gaps_sats.py**: This script estimates the number of AIS satellites within range of a vessel at every minute of the day for a given one degree grid.

**figs_model_selection.py**: Generates all model selection figures. See `model_selection/README.md` for how to get the data for the final model for the paper so this will run correctly. To run: `python -m figs_model_selection --lowest_rec 10 --run_double True`

**figs_reception_quality.py**: This script produces the figures of AIS reception quality included in the supplement.

**figs_time_lost_to_gaps_methods.py**: Generates Figs S15-17 (as labeled in initial Science Advances submission).

**figs_time_lost_to_gaps_results.py**: Generates Figs 1&2 and Fig S18 (as labeled in initial Science Advances submission).

**nearest_EEZ_code.R**: Calculates the % of fishing activity and the % of disabling events within 100 kms of every EEZ. Also calculates the % of total fishing activity and disabling events near EEZs with disputed boundaries.

**total_time_lost_to_gaps.py**: Builds the table that estimates the fraction of time lost to disabling events from different methods.
