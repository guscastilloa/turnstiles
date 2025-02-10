For tracking runtime behavior of our pipeline (NOT for testing logs). 
For example:

```
logs/application/
├── network_aggregation/
│   └── 2024-02-10/
│       ├── daily_processing.log
│       │   "Processed P2000_20160223.csv: 50,000 coincidences found"
│       │   "Memory usage: 4.2GB"
│       │   "WARNING: Missing timestamps in row 1205"
│       └── aggregation_stats.log
│           "Total edges in 3s window: 59,890,123"
├── feature_extraction/
│   └── 2024-02-10/
│       ├── temporal_features.log
│       │   "Computing hour-of-day distributions..."
│       └── spatial_features.log
└── model_training/
    └── 2024-02-10/
        ├── training_progress.log
        │   "Epoch 1: AUC=0.72, Loss=0.4"
        └── validation_results.log
```
