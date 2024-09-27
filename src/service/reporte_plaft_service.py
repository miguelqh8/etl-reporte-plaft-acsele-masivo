import datetime
from enum import Enum
from src.utils import config_app
from ..repository.reporte_plaft_repository import (
    obtener_ultimo_proceso_plaft,
    registrar_log_plaft_proceso_interno
)
import datetime
import pandas as pd
import numpy as np
from ..utils.logger import logger
import requests
import json

def reporte_plaft_service():
    logger.info("reporte_plaft_service_acsele_masivo - inicio")
    
    logger.info("reporte_plaft_service - Obteniendo ultimo codigo de proceso")
    idProceso = 0
    fechaCorte = ""
    proceso=obtener_ultimo_proceso_plaft()
    idProceso = proceso['id_proceso'].values[0]
    fechaCorte = proceso['fecha_corte'].values[0]
    logger.info("idProceso => " + str(idProceso))
    logger.info("fechaCorte => " + str(fechaCorte))
    anio = int(fechaCorte[0:4])
    mes = int(fechaCorte[4:6])
    dia = int(fechaCorte[6:8])
    fechaCorte = datetime.datetime(anio, mes, dia)    
    logger.info("fechaCorte datetime => " + str(fechaCorte))

    registrar_log_plaft_proceso_interno('ACSELE-MASIVO-CARGAR_TEMPORAL-INICIO')
    registrar_log_plaft_proceso_interno('00.CALCULANDO FECHAS')


    response = {
        "Message": "Mi trabajo aqui ha terminado :D" 
    }
    logger.info("reporte_plaft_service_acsele_masivo - fin")
    return response

def objeto_a_json(obj):
    # Si el objeto tiene un método __dict__, usa eso; de lo contrario, retorna una representación por defecto
    return obj.__dict__ if hasattr(obj, '__dict__') else str(obj)