# Backend

## Creating a Virtual Environment

- python3.13 -m venv venv

## Activating the Virtual Environment

- source venv/bin/activate

## Installing Dependencies

- pip3 install -r requirements.txt

## How to run (local)

### 1. Navigate to the application directory:

- cd src/

### 2. Run the server:

- uvicorn app.main:app --reload

## How to run (Docker)

### 1. Navigate to the Backend directory:

- cd Backend/

### 2. Build and run containers:

- docker compose up --build

## How to test

### 1. Navigate to the Backend directory:

- cd Backend/

### 2. Run tests:

- pytest -v

## Stopping the Server

- Ctrl + C

## Stopping Docker

- docker compose down

## Deactivating the Virtual Environment

- deactivate
