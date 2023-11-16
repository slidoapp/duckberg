In order to run these demos:
1. Create venv and actiavete it (eg. `python -m venv venv && source venv/bin/activate`)
2. Upgrade pip (eg. `pip install --upgrade pip`)
3. Install duckberg and `requirements.txt` (eg. `pip install -r requirements.txt && pip install -e ../.`)

4. Use try duckberg
    - Change `catalog_type` to match your setup

    ### or 

    - Use `minimal_iceberg_setup`
        - Go to `demo/minimal_iceberg_setup` (eg. `cd minimal_iceberg_setup`)
        - Run `setup.sh` - (eg. `chmod +x setup.sh && ./setup.sh`)
        - Wait few seconds (eg. 30s) for it to be fully setup
        - Return to /demo (eg. `cd ..`)
        - Run and play with examples (eg. `python simple_query.py`)