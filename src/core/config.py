from pydantic import EmailStr, BaseModel
class ApiSettings(BaseModel):
    title: str
    description: str
    version: str
    contact_name: str
    contact_email: EmailStr
    contact_url: str