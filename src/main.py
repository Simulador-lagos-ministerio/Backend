import fastapi as _fastapi

import services as _services

app = _fastapi.FastAPI()

_services.create_database()