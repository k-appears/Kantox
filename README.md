## Steps to run the project
1. Install python3 and pip3
2. Install virtualenv
    ```commandline
    pip3 install virtualenv
    ```
3. Decompress the data folder into the root of the project
4. Install the requirements
    ```commandline
    python3 -m venv ~/venv 
    source ~/venv/bin/activate 
    pip install -r requirements.txt
    ```
5. Add project root to PYTHONPATH
    ```commandline
    export PYTHONPATH=$PYTHONPATH:$(pwd)
    ```
6. Run the project
    ```commandline
    python3 src/main.py
    ```

