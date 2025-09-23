

#### AWS S3 버킷 디렉토리 구조
```
[AWS S3 real-estate-avm]/
├── raw/                   # [1단계] 원본 데이터 (Immutable)
│   ├── real-estate/
│   │   └── transactions/
│   │       └── dt=2025-09-23/transactions.json
│   ├── economic-indicators/
│   │   └── bosi/
│   │       └── dt=2025-09-01/bosi.xml
│   └── satellite-images/
│       └── gangnam/
│           └── dt=2025-09-01/original_image.tif
│
├── processed/             # [2단계] 가공된 데이터
│   ├── normalized-images/
│   │   └── gangnam/
│   │       └── dt=2025-09-01/normalized_image.png
│   ├── augmented-images/
│   │   └── gangnam/
│   │       └── dt=2025-09-01/augmented_image_flip.png
│   ├── image-vectors/
│   │   └── gangnam/
│   │       └── dt=2025-09-01/vectors.parquet
│   └── marts/
│       └── ml-features/
│           └── dt=2025-09-23/ml_features.parquet
│
├── artifacts/             # [산출물] ML 모델, 스케일러 등
│   ├── models/
│   │   └── version=1.0.0/resnet50_feature_extractor.h5
│   └── scalers/
│       └── version=1.0.0/price_scaler.pkl
│
```
