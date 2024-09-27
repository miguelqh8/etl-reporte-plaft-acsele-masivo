from decouple import config
from urllib.parse import quote_plus

class Config:
    ALLOY_USER = config('ALLOY_USER')
    ALLOY_PASSWORD = config('ALLOY_PASSWORD')
    ALLOY_HOST = config('ALLOY_HOST')
    ALLOY_PORT = config('ALLOY_PORT')
    ALLOY_DATABASE = config('ALLOY_DATABASE')

    PLAFT_USER = config('PLAFT_USER')
    PLAFT_PASSWORD = config('PLAFT_PASSWORD')
    PLAFT_HOST = config('PLAFT_HOST')
    PLAFT_PORT = config('PLAFT_PORT')
    PLAFT_DATABASE = config('PLAFT_DATABASE')

    # Alloy principal
    ALLOY_URI = f"postgresql://{ALLOY_USER}:{quote_plus(ALLOY_PASSWORD)}@{ALLOY_HOST}:{ALLOY_PORT}/{ALLOY_DATABASE}"
    
    # Plaft
    PLAFT_URI = f"postgresql://{PLAFT_USER}:{quote_plus(PLAFT_PASSWORD)}@{PLAFT_HOST}:{PLAFT_PORT}/{PLAFT_DATABASE}"