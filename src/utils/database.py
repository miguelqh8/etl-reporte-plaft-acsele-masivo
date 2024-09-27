from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.utils import config_app

engines  = {
    'alloy': create_engine(config_app.Config.ALLOY_URI),
    'plaft': create_engine(config_app.Config.PLAFT_URI),
}

SessionLocal = sessionmaker(bind=engines['alloy'])
