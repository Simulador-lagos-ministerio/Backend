# Backend
## How to run

python3 -m venv venv
source venv/bin/activate
pip3 install passlib[bcrypt] SQLAlchemy fastapi[all] pyjwt
cd src/
uvicorn main:app --reload