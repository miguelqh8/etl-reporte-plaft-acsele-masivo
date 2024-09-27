from ..utils.database_executes import bulk_insert_from_df_mssql, execute_query_to_df,bulk_insert_from_df,execute_query_no_results,execute_query_with_results
from ..utils.logger import logger
import pandas as pd

def obtener_ultimo_proceso_plaft():
    logger.info('obtener_ultimo_proceso_plaft - inicio')
    try:
        dfAlloy = execute_query_to_df("select id_programacion as id_proceso , estado as estado_proceso , to_char(fecha_corte, 'YYYYMMDD') as fecha_corte from PROGRAMACION_REPORTE order by id_programacion desc limit 1", 'plaft')
        logger.info(f"obtener_ultimo_proceso_plaft - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en obtener_ultimo_proceso_plaft: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('obtener_ultimo_proceso_plaft - fin')
    return dfAlloy

def obtener_calificacion_variable_detalleo_plaft():
    logger.info('obtener_calificacion_variable_detalleo_plaft - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT TRANSLATE( NOMBRE, 'áÁéÉíÍóÓúÚ', 'aAeEiIoOuU') AS nombre, idcalificacion_riesgo FROM calificacion_variable_detalle WHERE idcalificacion_variable = 14", 'plaft')
        logger.info(f"obtener_calificacion_variable_detalleo_plaft - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en obtener_calificacion_variable_detalleo_plaft: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('obtener_calificacion_variable_detalleo_plaft - fin')
    return dfAlloy

def registrar_log_proceso_alloy(p_fecha_corte):
    logger.info('registrar_log_proceso_plaft - inicio')
    try:
        query=f"SELECT interseguror.pck_reportes_plaft_usp_registrar_log_proceso( 0, 'REPORTE_PLAFT', '', 'Inicia proceso general', 'PLAFT', '{p_fecha_corte}' :: TIMESTAMP)"
        logger.info(query)
        dfAlloy = execute_query_to_df(query, 'alloy')
        logger.info(f"registrar_log_proceso_plaft - => {str(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en registrar_log_proceso_plaft: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('registrar_log_proceso_plaft - fin')
    return dfAlloy

def registrar_log_proceso_alloy_det(id_log,nombre,etapa,log):
    logger.info(f'registrar_log_proceso_alloy_det {nombre}.{etapa}.{log}- inicio')
    try:
        query=f"SELECT interseguror.pck_reportes_plaft_usp_registrar_log_proceso_detalle({id_log}, '{nombre}', '{etapa}', '{log}', 'PLAFT');"
        logger.info(query)
        dfAlloy = execute_query_to_df(query, 'alloy')
        logger.info(f"registrar_log_proceso_alloy_det {nombre}.{etapa}.{log} - => {str(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en registrar_log_proceso_alloy_det: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('registrar_log_proceso_alloy_det {nombre}.{etapa}.{log} - fin')
    return dfAlloy

def actualiza_estado_proceso_plaft(id_proceso):
  logger.info('actualiza_estado_proceso_plaft - inicio')
  res=execute_query_with_results(f"update PROGRAMACION_REPORTE set estado = 2,  fecha_inicio = now() where id_programacion = {id_proceso}",'plaft')
  logger.info('actualiza_estado_proceso_plaft - fin')
  return res

def actualiza_UBIGEO_plaft(nivelRiesgo,departamento):
  logger.info('actualiza_estado_proceso_plaft - inicio')
  res=execute_query_with_results(f"UPDATE UBIGEO SET RIESGO = '{nivelRiesgo}' WHERE UPPER(DSC_DEPARTAMENTO) = '{departamento}'",'plaft')
  logger.info('actualiza_UBIGEO_plaft - fin')
  return res

def limpia_datos_alloy():
  logger.info('limpia_datos_alloy - inicio')
  res=execute_query_with_results(f"SELECT pck_reportes_plaft_usp_limpiar_temporales()",'alloy')
  logger.info('limpia_datos_alloy - fin')
  return res

def limpiar_actividad_economica_plaft():
    logger.info('limpiar_actividad_economica_plaft - inicio')
    try:
        dfAlloy = execute_query_no_results("DELETE FROM actividad_economica;", 'plaft')
        logger.info(f"limpiar_actividad_economica_plaft - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en limpiar_actividad_economica_plaft: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('limpiar_actividad_economica_plaft - fin')
    return dfAlloy

def consultar_equivalencia_alloy():
    logger.info('consultar_equivalencia_plaft - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'PLAFT' AND E.TIPO = 'TIPODOCUMENTO'", 'alloy')
        logger.info(f"consultar_equivalencia_plaft - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en obtener_ultimo_proceso_plaft: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_equivalencia_plaft - fin')
    return dfAlloy

def consultar_lista_negra_plaft():
    logger.info('consultar_lista_negra_plaft - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT DISTINCT ultimos_registros.idtipo_documento, ultimos_registros.documento, cliente_calificacion.nivel_riesgo, C .cumulo FROM( SELECT cliente.idcliente, tipo_documento.idtipo_documento, cliente.documento, MAX ( cliente_calificacion.fecha_creacion) AS fecha_creacion FROM cliente_calificacion INNER JOIN cliente ON cliente_calificacion.idcliente = cliente.idcliente INNER JOIN tipo_documento ON tipo_documento.idtipo_documento = cliente.idtipo_documento LEFT JOIN cliente_poliza_propuesta ON cliente_poliza_propuesta.idcliente_poliza_propuesta = cliente_calificacion.idcliente_poliza_propuesta WHERE cliente.activo = 1 GROUP BY cliente.idcliente, tipo_documento.idtipo_documento, cliente.documento ) ultimos_registros INNER JOIN cliente_calificacion ON ( ultimos_registros.idcliente = cliente_calificacion.idcliente AND ultimos_registros.fecha_creacion = cliente_calificacion.fecha_creacion ) INNER JOIN cliente C ON C .idcliente = ultimos_registros.idcliente", 'plaft')
        logger.info(f"consultar_lista_negra_plaft - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en obtener_ultimo_proceso_plaft: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_lista_negra_plaft - fin')
    return dfAlloy

def insertar_lista_negra_d_alloy(df):
  logger.info('insertar_lista_negra_d_plaft - inicio')
  columns = ('cod_tipo_documento', 'numero_documento', 'calificacion', 'cumulo')
  bulk_insert_from_df(df, 'plaft_d_lista_negra','interseguror',columns,'alloy')
  logger.info('insertar_lista_negra_d_plaft - fin')
  return True

def consultar_ppe_plaft():
    logger.info('consultar_ppe_plaft - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT DISTINCT base.idtipo_documento, base.documento FROM base INNER JOIN base_categoria ON base.idbase_categoria = base_categoria.idbase_categoria WHERE base_categoria.idbase_tipo = 1 AND base_categoria.idbase_categoria = 1", 'plaft')
        logger.info(f"consultar_ppe_plaft - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en obtener_ultimo_proceso_plaft: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_ppe_plaft - fin')
    return dfAlloy

def insertar_plaft_d_ppe_alloy(df):
  logger.info('insertar_plaft_d_ppe_plaft - inicio')
  columns = ('cod_tipo_documento', 'numero_documento')
  bulk_insert_from_df(df, 'plaft_d_ppe','interseguror',columns,'alloy')
  logger.info('insertar_plaft_d_ppe_plaft - fin')
  return True

def consultar_actividad_economica_alloy():
    logger.info('consultar_actividad_economica_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("select * from interseguror.plaft_d_actividad_economica  WHERE ID_ACTIVIDADECONOMICA<>-1", 'alloy')
        logger.info(f"consultar_actividad_economica_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_actividad_economica_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_actividad_economica_alloy - fin')
    return dfAlloy

def insertar_act_economica_plaft(df):
  logger.info(f'insertar_act_economica_plaft - inicio')
  columns = [
        'id_act_economica','descripcion','riesgo','ciiu','estado','usuario_creacion'
    ]
  bulk_insert_from_df(df, 'actividad_economica','public',columns,'plaft',">")
  logger.info('insertar_plaft_d_ppe_plaft - fin')
  return True

def limpiar_plaft_d_producto_alloy():
    logger.info('limpiar_actividad_economica_plaft - inicio')
    try:
        dfAlloy=execute_query_no_results("DELETE FROM interseguror.plaft_d_producto;", 'alloy')
    except Exception as e:
        logger.error(f"Error en limpiar_plaft_d_producto_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('limpiar_plaft_d_producto_alloy - fin')
    return dfAlloy

def obtener_producto_universo_plaft():
    logger.info('obtener_producto_universo_plaft - inicio')
    try:
        dfAlloy = execute_query_to_df("select * from producto_universo", 'plaft')
        logger.info(f"obtener_producto_universo_plaft - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en obtener_producto_universo_plaft: {str(e)}")
        dfAlloy = pd.DataFrame()
    logger.info('obtener_producto_universo_plaft - fin')
    return dfAlloy

def insertar_plaft_d_producto_alloy(df):
  logger.info('insertar_plaft_d_producto_alloy - inicio')
  columns = ('id_producto_key', 'origen', 'cod_ramo', 'cod_subramo', 'cod_producto', 'cod_producto_sbs', 'id_riesgo_sbs', 'desc_ramo', 'desc_subramo', 'desc_producto', 'id_regimen', 'calculo_prima', 'id_identificacion_plaft', 'ind_colectivo')
  bulk_insert_from_df(df, 'plaft_d_producto','interseguror',columns,'alloy')
  logger.info('insertar_plaft_d_producto_alloy - fin')
  return True

def limpiar_transaccional_alloy():
    logger.info('limpiar_transaccional_alloy - inicio')
    try:
        dfAlloy=execute_query_no_results("TRUNCATE TABLE interseguror.plaft_transaccional_20181231", 'alloy')
    except Exception as e:
        logger.error(f"Error en limpiar_transaccional_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('limpiar_transaccional_alloy - fin')
    return dfAlloy

def plaft_reporte_exp_serv(fec_corte):
    logger.info('plaft_reporte_exp_serv - inicio')
    try:
        dfAlloy=execute_query_to_df(f"EXEC USP_Plaft_Reporte_ExpServ '{fec_corte}'", 'exp_serv')
    except Exception as e:
        logger.error(f"Error en plaft_reporte_exp_serv: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('plaft_reporte_exp_serv - fin')
    return dfAlloy

def plaft_reporte_rviadm(fec_corte):
    logger.info('plaft_reporte_rviadm - inicio')
    try:
        dfAlloy=execute_query_to_df(f"EXEC USP_Plaft_Reporte_RV '{fec_corte}'", 'rviadm')
    except Exception as e:
        logger.error(f"Error en plaft_reporte_rviadm: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('plaft_reporte_rviadm - fin')
    return dfAlloy

def plaft_reporte_admwr(fec_corte):
    logger.info('plaft_reporte_admwr - inicio')
    try:
        dfAlloy=execute_query_to_df(f"EXEC USP_Plaft_Reporte_RPP '{fec_corte}'", 'admwr')
    except Exception as e:
        logger.error(f"Error en plaft_reporte_admwr: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('plaft_reporte_admwr - fin')
    return dfAlloy

def consultar_moneda_expserv_alloy():
    logger.info('consultar_moneda_expserv_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'EXPSERV' AND E.TIPO = 'MONEDA'", 'alloy')
        logger.info(f"consultar_moneda_expserv_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en obtener_ultimo_proceso_plaft: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_moneda_expserv_alloy - fin')
    return dfAlloy

def consultar_documento_expserv_alloy():
    logger.info('consultar_documento_expserv_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'EXPSERV' AND E.TIPO = 'TIPODOCUMENTO'", 'alloy')
        logger.info(f"consultar_documento_expserv_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_documento_expserv_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_documento_expserv_alloy - fin')
    return dfAlloy

def consultar_departamento_expserv_alloy():
    logger.info('consultar_departamento_expserv_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'EXPSERV' AND E.TIPO = 'DEPARTAMENTO'", 'alloy')
        logger.info(f"consultar_departamento_expserv_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_departamento_expserv_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_departamento_expserv_alloy - fin')
    return dfAlloy

def consultar_producto_plaft_expserv_alloy():
    logger.info('consultar_producto_plaft_expserv_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("select * from interseguror.plaft_d_producto D where d.origen = 'EXPSERV'", 'alloy')
        logger.info(f"consultar_producto_plaft_expserv_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_producto_plaft_expserv_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_producto_plaft_expserv_alloy - fin')
    return dfAlloy

def insertar_plaft_transaccional_alloy(df,columns):
    logger.info('insertar_plaft_transaccional_alloy - inicio')
    bulk_insert_from_df(df, 'plaft_transaccional_20181231','interseguror',columns,'alloy',';')
    logger.info('insertar_plaft_transaccional_alloy - fin')
    return True

def usp_obtener_acsele_vida_alloy(fecha):
    logger.info(f'usp_obtener_acsele_vida_alloy- inicio')
    try:
        query=f"SELECT interseguror.pck_reportes_plaft_usp_obtener_acsele_vida('{fecha}');"
        logger.info(query)
        dfAlloy = execute_query_to_df(query, 'alloy')
        logger.info(f"usp_obtener_acsele_vida_alloy - => {str(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en usp_obtener_acsele_vida_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('usp_obtener_acsele_vida_alloy  - fin')
    return dfAlloy

def usp_obtener_acsele_masivo_alloy(fecha):
    logger.info(f'usp_obtener_acsele_masivo_alloy- inicio')
    try:
        query=f"SELECT interseguror.pck_reportes_plaft_usp_obtener_acsele_masivo('{fecha}');"
        logger.info(query)
        dfAlloy = execute_query_to_df(query, 'alloy')
        logger.info(f"usp_obtener_acsele_masivo_alloy - => {str(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en usp_obtener_acsele_vida_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('usp_obtener_acsele_masivo_alloy  - fin')
    return dfAlloy

def usp_retro_transaccional_alloy():
    logger.info(f'usp_retro_transaccional_alloy- inicio')
    try:
        query=f"SELECT interseguror.pck_reportes_plaft_usp_retro_transaccional();"
        logger.info(query)
        dfAlloy = execute_query_to_df(query, 'alloy')
        logger.info(f"usp_retro_transaccional_alloy - => {str(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en usp_obtener_acsele_vida_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('usp_retro_transaccional_alloy  - fin')
    return dfAlloy

def finalizar_estado_proceso_plaft(id_proceso):
    logger.info('finalizar_estado_proceso_plaft - inicio')
    res=execute_query_with_results(f"UPDATE PROGRAMACION_REPORTE set estado = 3, fecha_fin = now(), resultado = 'El proceso terminó correctamente', usuario_modificacion = 'plaft', fecha_modificacion = now() where id_programacion = {id_proceso}",'plaft')
    logger.info('finalizar_estado_proceso_plaft - fin')
    return res

def consultar_moneda_rviadm_alloy():
    logger.info('consultar_moneda_rviadm_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'RVIADM' AND E.TIPO = 'MONEDA'", 'alloy')
        logger.info(f"consultar_moneda_rviadm_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_moneda_rviadm_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_moneda_rviadm_alloy - fin')
    return dfAlloy

def consultar_documento_rviadm_alloy():
    logger.info('consultar_documento_rviadm_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'RVIADM' AND E.TIPO = 'TIPODOCUMENTO'", 'alloy')
        logger.info(f"consultar_documento_rviadm_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_documento_rviadm_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_documento_rviadm_alloy - fin')
    return dfAlloy

def consultar_departamento_rviadm_alloy():
    logger.info('consultar_departamento_rviadm_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'RVIADM' AND E.TIPO = 'DEPARTAMENTO'", 'alloy')
        logger.info(f"consultar_departamento_rviadm_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_departamento_rviadm_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_departamento_rviadm_alloy - fin')
    return dfAlloy

def consultar_producto_plaft_rviadm_alloy():
    logger.info('consultar_producto_plaft_rviadm_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("select * from interseguror.plaft_d_producto D where d.origen = 'RVIADM'", 'alloy')
        logger.info(f"consultar_producto_plaft_rviadm_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_producto_plaft_rviadm_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_producto_plaft_rviadm_alloy - fin')
    return dfAlloy

def consultar_moneda_admwr_alloy():
    logger.info('consultar_moneda_admwr_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'ADMWR' AND E.TIPO = 'MONEDA'", 'alloy')
        logger.info(f"consultar_moneda_admwr_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_moneda_admwr_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_moneda_admwr_alloy - fin')
    return dfAlloy

def consultar_documento_admwr_alloy():
    logger.info('consultar_documento_admwr_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'ADMWR' AND E.TIPO = 'TIPODOCUMENTO'", 'alloy')
        logger.info(f"consultar_documento_admwr_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_documento_admwr_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_documento_admwr_alloy - fin')
    return dfAlloy

def consultar_departamento_admwr_alloy():
    logger.info('consultar_departamento_admwr_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'ADMWR' AND E.TIPO = 'DEPARTAMENTO'", 'alloy')
        logger.info(f"consultar_departamento_admwr_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_departamento_admwr_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_departamento_admwr_alloy - fin')
    return dfAlloy

def consultar_producto_plaft_admwr_alloy():
    logger.info('consultar_producto_plaft_admwr_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("select * from interseguror.plaft_d_producto D where d.origen = 'ADMWR'", 'alloy')
        logger.info(f"consultar_producto_plaft_admwr_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_producto_plaft_admwr_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_producto_plaft_admwr_alloy - fin')
    return dfAlloy

def consultar_moneda_digital_alloy():
    logger.info('consultar_moneda_digital_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'DIGITAL' AND E.TIPO = 'MONEDA'", 'alloy')
        logger.info(f"consultar_moneda_digital_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_moneda_digital_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_moneda_digital_alloy - fin')
    return dfAlloy

def consultar_documento_digital_alloy():
    logger.info('consultar_documento_digital_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT E.CODIGO_ORIGEN, E.CODIGO_EQUIVALENTE FROM interseguror.PLAFT_EQUIVALENCIA E WHERE E.ORIGEN = 'DIGITAL' AND E.TIPO = 'TIPODOCUMENTO'", 'alloy')
        logger.info(f"consultar_documento_digital_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_documento_digital_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera x
    logger.info('consultar_documento_digital_alloy - fin')
    return dfAlloy

def consultar_producto_plaft_digital_alloy():
    logger.info('consultar_producto_plaft_digital_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("select * from interseguror.plaft_d_producto D where d.origen = 'DIGITAL'", 'alloy')
        logger.info(f"consultar_producto_plaft_digital_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_producto_plaft_digital_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_producto_plaft_digital_alloy - fin')
    return dfAlloy

######################################3

def registrar_log_plaft_proceso_interno(descripcion):
    logger.info('registrar_log_plaft_proceso_interno - inicio')
    try:
        query=f"INSERT INTO INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.SEQ_PLAFT_LOG_PRO_INT'),'{descripcion}', LOCALTIMESTAMP);"
        logger.info(query)
        dfAlloy = execute_query_with_results(query, 'alloy')
        logger.info(f"registrar_log_plaft_proceso_interno - => {str(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en registrar_log_plaft_proceso_interno: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('registrar_log_plaft_proceso_interno - fin')
    return dfAlloy