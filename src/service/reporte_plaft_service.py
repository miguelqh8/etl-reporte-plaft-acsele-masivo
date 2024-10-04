import datetime
from enum import Enum
from src.utils import config_app
from ..repository.reporte_plaft_repository import (
    obtener_ultimo_proceso_plaft,
    registrar_log_plaft_proceso_interno,
    truncate_temporales,
    insertar_tmp_direcciones_masivos,
    poblar_tmp_direcciones_masivos,
    poblar_uni_masivos,
    poblar_pre_contra_mas,
    poblar_pre_aseg_mas,
    poblar_mig_contratante_mas_n,
    poblar_mig_contratante_mas_j,
    poblar_mig_asegurado_mas,
    poblar_mig_asegurado_mas2,
    poblar_mig_asegurado_mas3,
    poblar_mig_asegurado_mas4,
    poblar_mig_asegurado_mas5,
    poblar_tmp_uni_benef_mas,
    poblar_mig_benef_sin_mas,
    poblar_tmp_montos,
    querys_filtros,
    poblar_productos,
    poblar_final_masivos,
    actualizar_actividad_economica
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
    fechaCorte_fin_str = fechaCorte_d_fin.strftime('%Y-%m-%d')
    fecha_inicio_recaudacion_d = fechaCorte_d - datetime.timedelta(days=1) - datetime.timedelta(days=365)
    fecha_inicio_recaudacion_str = fecha_inicio_recaudacion_d.strftime('%Y-%m-%d')
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
    poblar_uni_masivos(fechaCorte_str2)
    
    registrar_log_plaft_proceso_interno(f'02.2.DIRECCIONES MASIVOS')
    poblar_tmp_direcciones_masivos()
    poblar_pre_contra_mas()
    poblar_pre_aseg_mas()
    
    registrar_log_plaft_proceso_interno(f'02.3.CONTRATANTES NATURAL')
    poblar_mig_contratante_mas_n()
    
    registrar_log_plaft_proceso_interno(f'02.4.CONTRATANTES JURIDICO')
    poblar_mig_contratante_mas_j()
    
    registrar_log_plaft_proceso_interno(f'02.5.ASEGURADO')
    poblar_mig_asegurado_mas()
    
    registrar_log_plaft_proceso_interno(f'02.6.ADD ASEGURADOS DESGPERSONAL')
    poblar_mig_asegurado_mas2()

    registrar_log_plaft_proceso_interno(f'02.7.ADD ASEGURADOS DESGPERSONAL 2')
    poblar_mig_asegurado_mas3()
    
    registrar_log_plaft_proceso_interno(f'02.8.ADD ASEGURADOS 2')
    poblar_mig_asegurado_mas4()
    
    registrar_log_plaft_proceso_interno(f'02.9.ADD ASEGURADOS ESP')
    poblar_mig_asegurado_mas5()
    
    registrar_log_plaft_proceso_interno(f'02.10.BENEFICIARIOS SINIESTROS')
    poblar_tmp_uni_benef_mas(fechaCorte_str2)
    
    registrar_log_plaft_proceso_interno(f'02.11.BENEFICIARIOS')
    poblar_mig_benef_sin_mas()
    
    registrar_log_plaft_proceso_interno(f'03. CARGAR TABLAS TEMPORALES MONTOS')
    poblar_tmp_montos(fechaCorte_str2,fecha_inicio_recaudacion_str,fechaCorte_fin_str)
    
    registrar_log_plaft_proceso_interno(f'04. UNIFICANDO PERSONAS')
    
    registrar_log_plaft_proceso_interno(f'05. FILTROS')
    querys_filtros(fechaCorte_str2)
    
    registrar_log_plaft_proceso_interno(f'06. PRODUCTOS DEFINICION')
    poblar_productos()
    
    registrar_log_plaft_proceso_interno(f'07. FINAL MASIVOS')
    poblar_final_masivos()
    
    registrar_log_plaft_proceso_interno(f'08. ACTIVIDAD ECONOMICA')
    actualizar_actividad_economica()
    
    registrar_log_plaft_proceso_interno(f'ACSELE-MASIVO-CARGAR_TEMPORAL-FIN')
    
    response = {
        "Message": "Mi trabajo aqui ha terminado :D" 
    }
    logger.info("reporte_plaft_service_acsele_masivo - fin")
    return response

def objeto_a_json(obj):
    # Si el objeto tiene un método __dict__, usa eso; de lo contrario, retorna una representación por defecto
    return obj.__dict__ if hasattr(obj, '__dict__') else str(obj)