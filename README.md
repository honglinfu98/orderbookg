# Orderbook

### Set up the repo

#### Create a python virtual environment

- macOS

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r dev-requirements.txt
pip3 install -e .
export $(cat .env | xargs)
```

- Windows

```bash
python -m venv venv
venv\Scripts\activate.bat
pip install -r requirements.txt
pip install -r dev-requirements.txt
pip install -e .
export $(cat .env | xargs)
```

### For running 


