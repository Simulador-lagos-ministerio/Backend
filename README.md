# Backend

## Creating a Virtual Environment

- python3 -m venv venv

## Activating the Virtual Environment

- source venv/bin/activate

### Installing Dependencies

- pip3 install -r requirements.txt

## How to run

### 1. Navigate to the application directory:

- cd src/

### 2. Run the server:

- uvicorn main:app --reload

## How to test (after install)

### 1. Navigate to the application directory:

- cd Backend/

### 2. Run tests:

- pytest -v

## Stopping the Server

- Ctrl + C

## Deactivating the Virtual Environment

- deactivate
