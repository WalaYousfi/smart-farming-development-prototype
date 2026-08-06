## Processing-Time Experiment

### Objective

Measure the execution-time stability of heterogeneous Silver
integration and integrated Gold anomaly detection.

### Configuration

- Field observations: 500
- Weather observations: 500
- Integration coverage: 100%
- Repetitions: 3 for preliminary testing
- Isolation Forest contamination: 0.05
- Isolation Forest estimators: 200
- Random seed: 42
- Execution environment: local Docker-based prototype

### Results

| Stage | Mean (s) | Median (s) | Minimum (s) | Maximum (s) | Standard deviation (s) |
|---|---:|---:|---:|---:|---:|
| Field–Weather integration | 2.8349 | 2.7388 | 2.6726 | 3.0933 | 0.2262 |
| Integrated Gold processing | 5.4846 | 5.4174 | 5.2804 | 5.7560 | 0.2448 |
| Combined | 8.3195 | 8.1562 | 7.9530 | 8.8493 | 0.4699 |

The preliminary three-run experiment shows that the Field–Weather
integration stage required an average of 2.8349 seconds, while the
Integrated Gold anomaly-detection stage required an average of
5.4846 seconds. The complete integration and Gold-processing
workflow required an average of 8.3195 seconds.

The standard deviations were 0.2262 seconds for integration,
0.2448 seconds for Gold processing, and 0.4699 seconds for the
combined workflow. These relatively small variations indicate
reasonably stable execution times across the three preliminary runs
under the same local experimental conditions.

The Integrated Gold stage was the most time-consuming part of the
measured workflow. Its mean execution time was approximately 1.93
times the integration-stage mean, which is consistent with the
additional work required for feature preparation, Isolation Forest
training, anomaly scoring, and the generation of analytical and
consumer-oriented Gold outputs.

### Experiment artifact

The complete machine-generated experiment report is stored at:

`experiments/prototype-v2/repeated-runs/timing-test-3-runs_20260806T225321Z.json`
### Preliminary interpretation

To be completed after the timing values are generated.

### Limitations

- Preliminary results use only three repetitions.
- The dataset contains 500 records per source.
- Tests run locally on one machine.
- Results do not represent distributed execution.