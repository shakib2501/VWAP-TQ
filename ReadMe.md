# NASDAQ ITCH5.0 Parser

This project processes NASDAQ ITCH data and calculates the hourly VWAP (Volume Weighted Average Price) for each stock.

## Usage

### 1. Input File
Place the NASDAQ ITCH data file in the `datafile` directory located at the project root: ./datafile/01302019.NASDAQ_ITCH50.gz
```commandline
../datafile/01302019.NASDAQ_ITCH50.gz
```

### 2. Output File
The output file containing the hourly VWAP will be generated in the `output` directory at the project root. If the directory does not exist, it will be created automatically:
```commandline
./output/hourly_vwap.csv
```

### 3. Running the Parser
To run the parser, execute the main script:
```bash
python src/nasdaq-itch-parser.py
