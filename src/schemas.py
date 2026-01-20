import pydantic as _pydantic

class UserBase(_pydantic.BaseModel):
    email: str