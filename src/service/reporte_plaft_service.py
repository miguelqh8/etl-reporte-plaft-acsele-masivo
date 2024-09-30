import datetime
from enum import Enum
from src.utils import config_app
from ..repository.reporte_plaft_repository import (
    obtener_ultimo_proceso_plaft,
    registrar_log_plaft_proceso_interno,
    truncate_temporales,
    consultar_uni_masivos
)
import datetime
import pandas as pd
import numpy as np
from ..utils.logger import logger
import requests
import json
import threading

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

    registrar_log_plaft_proceso_interno('ACSELE-MASIVO-CARGAR_TEMPORAL-INICIO')
    registrar_log_plaft_proceso_interno('00.CALCULANDO FECHAS')
    logger.info("fechaCorte datetime => " + str(fechaCorte))
    fechaCorte_d = fechaCorte + datetime.timedelta(days=1)
    fechaCorte_str = fechaCorte_d.strftime('%d/%m/%Y')
    fechaCorte_str2 = fechaCorte_d.strftime('%Y-%m-%d')
    fechaCorte_d_fin = fechaCorte_d - datetime.timedelta(days=1)
    fechaCorte_fin_str = fechaCorte_d_fin.strftime('%d/%m/%Y')
    fecha_inicio_recaudacion_d = fechaCorte_d - datetime.timedelta(days=1) - datetime.timedelta(days=365)
    fecha_inicio_recaudacion_str = fecha_inicio_recaudacion_d.strftime('%d/%m/%Y')
    logger.info("fechaCorte => " + fechaCorte_str)
    logger.info("fechaCorteFin => " + fechaCorte_fin_str)
    logger.info("fechaInicioRecaudacion => " + fecha_inicio_recaudacion_str)
    registrar_log_plaft_proceso_interno(f'00.FECHA CORTE: {fechaCorte_str}')
    registrar_log_plaft_proceso_interno(f'00.FECHA CORTE FIN: {fechaCorte_fin_str}')
    registrar_log_plaft_proceso_interno(f'00.FECHA INICIO RECAUDACION:: {fecha_inicio_recaudacion_str}')

    registrar_log_plaft_proceso_interno(f'01.ELIMINANDO TABLAS')
    tables = [
    "TBL_UNI_MASIVOS", "PRE_DIR_TOTAL", "TMP_DIR_TOTAL", "PRE_CONTR_MAS", "PRE_ASEG_MAS",
    "MIG_CONTRATANTE_MAS_N", "MIG_ASEGURADO_MAS", "MIG_CONTRATANTE_MAS_J", "TMP_OP_MASIVOS",
    "TBL_PERSONAS_MASIVOS", "TMP_OP_PH_MASIVOS", "TMP_OP_MASIVOS_PH_MONTO", "TMP_OP_PH_MASIVOS_V2",
    "TMP_OP_MASIVOS_PH_MONTO_V2", "TMP_UNI_BENEF_MAS", "MIG_BENEF_SIN_MAS", "RPT_FINAL_MASIVOS",
    "TMP_REP_CLASS_PRPODUCT", "TMP_OP_MASIVOS_RECAUDADO"
    ]
    threads = []
    for table in tables:
        thread = threading.Thread(target=truncate_temporales, args=(table,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    
    registrar_log_plaft_proceso_interno(f'02.CARGAR TABLAS TEMPORALES')
    registrar_log_plaft_proceso_interno(f'02.1.POLIZAS MASIVOS')
    masivos=consultar_uni_masivos(fechaCorte_str2)
    masivos = masivos.sort_values(by=['NUMEROPOLIZAINPUT', 'ID'], ascending=[True, False])
    masivos['ROW_NUMBER'] = masivos.groupby('NUMEROPOLIZAINPUT').cumcount() + 1
    df_filtered_masivos = masivos[(masivos['ROW_NUMBER'] == 1) & (masivos['ESTADO'] == 'Vigente')]
    df_filtered_masivos['FLAG_CARGA'] = 1
    print(df_filtered_masivos[['ID', 'ITEM', 'ESTADO', 'PRODUCTO', 'NUMEROPOLIZAINPUT', 'FECHACARGASISTEMAINPUT','NOMBREARCHIVOTRAMAINPUT', 'FLAG_CARGA', 'INITIALDATE', 'FINISHDATE']])
    df_filtered_masivos.to_csv('resultados_pandas.csv', index=False)
    
    response = {
        "Message": "Mi trabajo aqui ha terminado :D" 
    }
    logger.info("reporte_plaft_service_acsele_masivo - fin")
    return response

def objeto_a_json(obj):
    # Si el objeto tiene un método __dict__, usa eso; de lo contrario, retorna una representación por defecto
    return obj.__dict__ if hasattr(obj, '__dict__') else str(obj)