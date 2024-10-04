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

def truncate_temporales(tabla):
    logger.info(f'truncate_temporales {tabla} - inicio')
    res=execute_query_with_results(f"TRUNCATE TABLE INTERSEGUROR.{tabla};",'alloy')
    logger.info(f'truncate_temporales {tabla} - fin')
    return res

def consultar_uni_masivos(fecha):
    logger.info('consultar_uni_masivos - inicio')
    try:
        query = f"""
        SELECT
            CTX.ID,
            CTX.ITEM,
            ST.DESCRIPTION AS ESTADO,
            PRO.DESCRIPTION AS PRODUCTO,
            PREP.NUMEROPOLIZAINPUT,
            PREP.FECHACARGASISTEMAINPUT,
            PREP.NOMBREARCHIVOTRAMAINPUT,
            PDCO.INITIALDATE,
            PDCO.FINISHDATE
        FROM INTERSEGURO.PREPOLICY PREP
        INNER JOIN INTERSEGURO.POLICYDCO PDCO ON PREP.PK = PDCO.DCOID
        INNER JOIN INTERSEGURO.CONTEXTOPERATION CTX ON CTX.ID = PDCO.OPERATIONPK
        INNER JOIN INTERSEGURO.STATE ST ON ST.STATEID = PDCO.STATEID
        INNER JOIN INTERSEGURO.AGREGATEDPOLICY AP ON CTX.ITEM = AP.AGREGATEDPOLICYID
        INNER JOIN INTERSEGURO.PRODUCT PRO ON PRO.PRODUCTID = AP.PRODUCTID
        INNER JOIN INTERSEGURO.PRODUCTPROPERTY PRE ON PRE.PRO_ID = PRO.PRODUCTID
        WHERE PRE.PRP_TYPE != 2 
          AND CTX.STATUS = 2  
          AND CTX.TIME_STAMP < '{fecha}'
          AND ST.DESCRIPTION = 'Vigente' 
          LIMIT 1000000 OFFSET 0;
        """
        dfAlloy = execute_query_to_df(query, 'alloy')
        logger.info(f"consultar_uni_masivos - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_uni_masivos: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_uni_masivos - fin')
    return dfAlloy

def consultar_uni_masivos_batch(fecha, batch_size=1000000):
    logger.info('consultar_uni_masivos - inicio')
    dfAlloy_chunks = []  # Lista para almacenar los resultados
    offset = 0  # Empezar desde la fila 0
    
    try:
        while True:
            # Crear la consulta con LIMIT y OFFSET
            query = f"""
            SELECT
                CTX.ID,
                CTX.ITEM,
                ST.DESCRIPTION AS ESTADO,
                PRO.DESCRIPTION AS PRODUCTO,
                PREP.NUMEROPOLIZAINPUT,
                PREP.FECHACARGASISTEMAINPUT,
                PREP.NOMBREARCHIVOTRAMAINPUT,
                PDCO.INITIALDATE,
                PDCO.FINISHDATE
            FROM INTERSEGURO.PREPOLICY PREP
            INNER JOIN INTERSEGURO.POLICYDCO PDCO ON PREP.PK = PDCO.DCOID
            INNER JOIN INTERSEGURO.CONTEXTOPERATION CTX ON CTX.ID = PDCO.OPERATIONPK
            INNER JOIN INTERSEGURO.STATE ST ON ST.STATEID = PDCO.STATEID
            INNER JOIN INTERSEGURO.AGREGATEDPOLICY AP ON CTX.ITEM = AP.AGREGATEDPOLICYID
            INNER JOIN INTERSEGURO.PRODUCT PRO ON PRO.PRODUCTID = AP.PRODUCTID
            INNER JOIN INTERSEGURO.PRODUCTPROPERTY PRE ON PRE.PRO_ID = PRO.PRODUCTID
            WHERE PRE.PRP_TYPE != 2 
              AND CTX.STATUS = 2  
              AND CTX.TIME_STAMP < '{fecha}'
              AND ST.DESCRIPTION = 'Vigente'
            LIMIT {batch_size} OFFSET {offset};
            """
            
            # Ejecutar la consulta y obtener un chunk de resultados
            df_chunk = execute_query_to_df(query, 'alloy')
            
            # Si no hay más filas, terminamos la consulta
            if df_chunk.empty:
                break

            # Agregar el chunk a la lista
            dfAlloy_chunks.append(df_chunk)
            
            # Aumentar el offset para la próxima iteración
            offset += batch_size
            logger.info(f"Procesado chunk desde offset {offset - batch_size} con {len(df_chunk)} filas")
        
        # Concatenar todos los chunks en un solo DataFrame
        dfAlloy = pd.concat(dfAlloy_chunks, ignore_index=True)
        logger.info(f"consultar_uni_masivos - total filas procesadas: {len(dfAlloy)}")
        
    except Exception as e:
        logger.error(f"Error en consultar_uni_masivos: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío en caso de error
    
    logger.info('consultar_uni_masivos - fin')
    return dfAlloy

def consultar_direcciones_masivos():
    logger.info('consultar_direcciones_masivos - inicio')
    try:
        query = f"SELECT MAX(AB1.IDDCO) AS IDDCO, AB1.TPT_ID FROM INTERSEGURO.STTE_THIRDPARTYADDRESSBOOK AB1 WHERE AB1.ISDEFAULTADDRESS = 1 GROUP BY AB1.TPT_ID"
        dfAlloy = execute_query_to_df(query, 'alloy')
        logger.info(f"consultar_direcciones_masivos - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en consultar_direcciones_masivos: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('consultar_direcciones_masivos - fin')
    return dfAlloy

def insertar_direcciones_masivos(df,columns):
    logger.info('insertar_direcciones_masivos - inicio')
    bulk_insert_from_df(df, 'pre_dir_total','interseguror',columns,'alloy',',')
    logger.info('insertar_direcciones_masivos - fin')
    return True

def poblar_tmp_direcciones_masivos():
    logger.info('poblar_tmp_direcciones_masivos - inicio')
    try:
        query = f"INSERT INTO interseguror.tmp_dir_total(PK, TPT_ID, DEPARTAMENTO, PROVINCIA, DISTRITO, COD_UBIGEO) WITH PRE_DIR_TOTAL AS ( SELECT MAX(AB1.IDDCO) AS IDDCO, AB1.TPT_ID FROM INTERSEGURO.STTE_THIRDPARTYADDRESSBOOK AB1 WHERE AB1.ISDEFAULTADDRESS = 1 GROUP BY AB1.TPT_ID) SELECT D.PK, AB.TPT_ID, U1.DESCRIPCION AS DEPARTAMENTO, U2.DESCRIPCION AS PROVINCIA, U3.DESCRIPCION AS DISTRITO, D.CODDISTRITOINPUT AS COD_UBIGEO FROM PRE_DIR_TOTAL AB LEFT JOIN INTERSEGURO.DIRECCION D ON D.PK = AB.IDDCO LEFT JOIN MIG_UBIGEO U1 ON U1.SIMBOLO = 'CodDepartamento' AND U1.CODIGO = D.CODDEPARTAMENTOINPUT LEFT JOIN MIG_UBIGEO U2 ON U2.SIMBOLO = 'CodProvincia' AND U2.CODIGO = D.CODPROVINCIAINPUT LEFT JOIN MIG_UBIGEO U3 ON U3.SIMBOLO = 'CodDistrito' AND U3.CODIGO = D.CODDISTRITOINPUT; "
        dfAlloy = execute_query_with_results(query, 'alloy')
    except Exception as e:
        logger.error(f"Error en consultar_tmp_direcciones_masivos: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('poblar_tmp_direcciones_masivos - fin')
    return dfAlloy

def insertar_tmp_direcciones_masivos(df,columns):
    logger.info('insertar_tmp_direcciones_masivos - inicio')
    bulk_insert_from_df(df, 'tmp_dir_total','interseguror',columns,'alloy',';')
    logger.info('insertar_tmp_direcciones_masivos - fin')
    return True

def poblar_uni_masivos(fecha):
    logger.info(f'poblar_uni_masivos - inicio')
    res=execute_query_with_results(f"WITH Filtrado AS( SELECT CTX.ID, CTX.ITEM, ST.DESCRIPTION AS ESTADO, PRO.DESCRIPTION AS PRODUCTO, PREP.NUMEROPOLIZAINPUT, PREP.FECHACARGASISTEMAINPUT, PREP.NOMBREARCHIVOTRAMAINPUT, PDCO.INITIALDATE, PDCO.FINISHDATE, ROW_NUMBER() OVER (PARTITION BY PREP.NUMEROPOLIZAINPUT ORDER BY CTX.ID DESC) AS NRO FROM INTERSEGURO.PREPOLICY PREP INNER JOIN INTERSEGURO.POLICYDCO PDCO ON PREP.PK = PDCO.DCOID INNER JOIN INTERSEGURO.CONTEXTOPERATION CTX ON CTX.ID = PDCO.OPERATIONPK INNER JOIN INTERSEGURO.STATE ST ON ST.STATEID = PDCO.STATEID INNER JOIN INTERSEGURO.AGREGATEDPOLICY AP ON CTX.ITEM = AP.AGREGATEDPOLICYID INNER JOIN INTERSEGURO.PRODUCT PRO ON PRO.PRODUCTID = AP.PRODUCTID INNER JOIN INTERSEGURO.PRODUCTPROPERTY PRE ON PRE.PRO_ID = PRO.PRODUCTID WHERE PRE.PRP_TYPE != 2 AND CTX.STATUS = 2 AND CTX.TIME_STAMP < to_timestamp('{fecha}', 'YYYY-MM-DD')) INSERT INTO TBL_UNI_MASIVOS ( ID, ITEM, ESTADO, PRODUCTO, NUMEROPOLIZAINPUT, FECHACARGASISTEMAINPUT, NOMBREARCHIVOTRAMAINPUT, FLAG_CARGA, INITIALDATE, FINISHDATE ) SELECT ID, ITEM, ESTADO, PRODUCTO, NUMEROPOLIZAINPUT, FECHACARGASISTEMAINPUT, NOMBREARCHIVOTRAMAINPUT, 1 AS FLAG_CARGA, INITIALDATE, FINISHDATE FROM Filtrado WHERE NRO = 1 AND ESTADO = 'Vigente';",'alloy')
    logger.info(f'poblar_uni_masivos - fin')
    return res

def poblar_pre_contra_mas():
    logger.info(f'poblar_pre_contra_mas - inicio')
    res=execute_query_with_results(f"insert into interseguror.PRE_CONTR_MAS SELECT P1.ITEM, POLPAR.THIRDPARTYID FROM TBL_UNI_MASIVOS P1 INNER JOIN INTERSEGURO.STPO_POLICYPARTICIPATIONDCO PDCO ON PDCO.STATUS != 4 AND PDCO.OPERATIONPK = P1. ID INNER JOIN INTERSEGURO.STPO_POLICYPARTICIPATION POLPAR ON POLPAR.AGREGATEDOBJECTID = PDCO.AGREGATEDOBJECTID AND POLPAR.ROL_ID = 8355 GROUP BY P1.ITEM, POLPAR.THIRDPARTYID;",'alloy')
    logger.info(f'poblar_pre_contra_mas - fin')
    return res

def poblar_pre_aseg_mas():
    logger.info(f'poblar_pre_aseg_mas - inicio')
    res=execute_query_with_results(f"insert into interseguror.PRE_ASEG_MAS SELECT P1.ITEM, IOPASE.THIRDPARTYID FROM TBL_UNI_MASIVOS P1 INNER JOIN INTERSEGURO.STPO_INSOBJPARTICIPATIONDCO PDCO ON PDCO.OPERATIONPK = P1.ID AND PDCO.STATUS != 4 INNER JOIN INTERSEGURO.STPO_INSOBJPARTICIPATION IOPASE ON PDCO.AGREGATEDOBJECTID = IOPASE.AGREGATEDOBJECTID AND IOPASE.ROL_ID = 6755 GROUP BY P1.ITEM, IOPASE.THIRDPARTYID;",'alloy')
    logger.info(f'poblar_pre_aseg_mas - fin')
    return res

def poblar_mig_contratante_mas_n():
    logger.info(f'poblar_mig_contratante_mas_n - inicio')
    res=execute_query_with_results(f"insert into interseguror.MIG_CONTRATANTE_MAS_N SELECT DISTINCT P1.ITEM AS IDPOLIZA, CAST( CASE WHEN PNAT1.PASAPORTEINPUT IS NOT NULL THEN 'PASAPORTE' WHEN PNAT1.CARNETDIPLOMATICOINPUT IS NOT NULL THEN 'CARNET DIPLOMATICO' WHEN PNAT1.CARNETEXTRANJERIAINPUT IS NOT NULL THEN 'CARNET EXTRANJERIA' WHEN PNAT1.CARNETFUERZASARMADASINPUT IS NOT NULL THEN 'CARNET FUERZAS ARMADAS' WHEN PNAT1.CARNETIDENPOLICIAINPUT IS NOT NULL THEN 'CARNET POLICIA' WHEN PNAT1.CARNETMINISTRABINPUT IS NOT NULL THEN 'CARNET MINISTERIO TRABAJO' WHEN PNAT1.DOCUMENTOIDENTIDADINPUT IS NOT NULL THEN 'DOCUMENTO DE IDENTIDAD' WHEN PNAT1.PARTIDANACIMIENTOINPUT IS NOT NULL THEN 'PARTIDA DE NACIMIENTO' WHEN PNAT1.LIBRETAMILITARINPUT IS NOT NULL THEN 'LIBRETA MILITAR' WHEN PNAT1.RUCNATURALINPUT IS NOT NULL THEN 'RUC NATURAL' ELSE'' END AS TEXT) AS TIPO_DOCUMENTO_CONT, CAST ( CASE WHEN PNAT1.PASAPORTEINPUT IS NOT NULL THEN PNAT1.PASAPORTEINPUT WHEN PNAT1.CARNETDIPLOMATICOINPUT IS NOT NULL THEN PNAT1.CARNETDIPLOMATICOINPUT WHEN PNAT1.CARNETEXTRANJERIAINPUT IS NOT NULL THEN PNAT1.CARNETEXTRANJERIAINPUT WHEN PNAT1.CARNETFUERZASARMADASINPUT IS NOT NULL THEN PNAT1.CARNETFUERZASARMADASINPUT WHEN PNAT1.CARNETIDENPOLICIAINPUT IS NOT NULL THEN PNAT1.CARNETIDENPOLICIAINPUT WHEN PNAT1.CARNETMINISTRABINPUT IS NOT NULL THEN PNAT1.CARNETMINISTRABINPUT WHEN PNAT1.DOCUMENTOIDENTIDADINPUT IS NOT NULL THEN PNAT1.DOCUMENTOIDENTIDADINPUT WHEN PNAT1.PARTIDANACIMIENTOINPUT IS NOT NULL THEN PNAT1.PARTIDANACIMIENTOINPUT WHEN PNAT1.LIBRETAMILITARINPUT IS NOT NULL THEN PNAT1.LIBRETAMILITARINPUT WHEN PNAT1.RUCNATURALINPUT IS NOT NULL THEN PNAT1.RUCNATURALINPUT ELSE'' END AS TEXT ) AS NUM_DOCUMENTO_CONT, CONCAT ( NULLIF ( RTRIM( NULLIF ( LTRIM( PNAT1.NOMBREINPUT ), '' ) ), '' ), CASE WHEN NULLIF ( RTRIM( NULLIF ( LTRIM( PNAT1.SEGUNDONOMBREINPUT ), '' ) ), '' ) IS NOT NULL THEN ' ' ELSE'' END ) AS NOMBRE_CONTR, NULLIF ( RTRIM( NULLIF ( LTRIM( PNAT1.APELLIDOINPUT ), '' ) ), '' ) AS APE_PATCONTR, NULLIF ( RTRIM( NULLIF ( LTRIM( PNAT1.APELLIDOMATERNOINPUT ), '' ) ), '' ) AS APE_MATCONTR, PRETH2.ACTIVIDADECONOMICAINPUT AS ACTIVIDAD_ECON, PNAT1.NACIONALIDADINPUT AS NACIONALIDAD, 'CONTRATANTE' AS TIPO_CLIENTE, 'NATURAL' AS TIPO_PERSONA, TD.DEPARTAMENTO, TD.PROVINCIA, TD.DISTRITO, TD.COD_UBIGEO, PNAT1.FECHANACIMIENTOINPUT AS FECNACIMIENTO, PNAT1.SEXOINPUT AS SEXO FROM TBL_UNI_MASIVOS P1 INNER JOIN PRE_CONTR_MAS POLPAR ON P1.ITEM = POLPAR.ITEM INNER JOIN INTERSEGURO.PERSONANATURAL PNAT1 ON POLPAR.THIRDPARTYID = PNAT1. STATIC INNER JOIN INTERSEGURO.PRETHIRDPARTY PRETH2 ON PRETH2.STATIC = PNAT1. STATIC LEFT JOIN TMP_DIR_TOTAL TD ON TD.TPT_ID = PRETH2.STATIC;",'alloy')
    logger.info(f'poblar_mig_contratante_mas_n - fin')
    return res

def poblar_mig_contratante_mas_j():
    logger.info(f'poblar_mig_contratante_mas_j - inicio')
    res=execute_query_with_results(f"INSERT INTO interseguror.MIG_CONTRATANTE_MAS_J SELECT DISTINCT P1.ITEM AS IDPOLIZA, 'RUC' AS TIPO_DOCUMENTO_CONT, PNAT1.RUCINPUT AS NUM_DOCUMENTO_CONT, CAST( NULLIF ( RTRIM( NULLIF ( LTRIM( PNAT1.NOMBRECOMPEMPINPUT), '' ) ), '' ) AS TEXT ) AS RAZON_SOCIAL, '' AS APE_PATCONTR, '' AS APE_MATCONTR, PRETH2.ACTIVIDADECONOMICAINPUT AS ACTIVIDAD_ECON, '' AS NACIONALIDAD, 'CONTRATANTE' AS TIPO_CLIENTE, 'JURIDICO' AS TIPO_PERSONA, TD.DEPARTAMENTO, TD.PROVINCIA, TD.DISTRITO, TD.COD_UBIGEO, PNAT1.FECHACONSTITUCIONINPUT AS FECNACIMIENTO, '' AS SEXO FROM TBL_UNI_MASIVOS P1 INNER JOIN PRE_CONTR_MAS POLPAR ON POLPAR.ITEM = P1.ITEM INNER JOIN INTERSEGURO.PRETHIRDPARTY PRETH2 ON PRETH2.STATIC = POLPAR.THIRDPARTYID INNER JOIN INTERSEGURO.PERSONAJURIDICA PNAT1 ON PNAT1.STATIC = PRETH2. STATIC LEFT JOIN TMP_DIR_TOTAL TD ON TD.TPT_ID = PRETH2.STATIC;",'alloy')
    logger.info(f'poblar_mig_contratante_mas_j - fin')
    return res

def poblar_mig_asegurado_mas():
    logger.info(f'poblar_mig_asegurado_mas - inicio')
    res=execute_query_with_results(f"insert into interseguror.MIG_ASEGURADO_MAS SELECT DISTINCT P1.ITEM AS IDPOLIZA , CAST(CASE WHEN PNAT1.PASAPORTEINPUT IS NOT NULL THEN 'PASAPORTE' WHEN PNAT1.CARNETDIPLOMATICOINPUT IS NOT NULL THEN 'CARNET DIPLOMATICO' WHEN PNAT1.CARNETEXTRANJERIAINPUT IS NOT NULL THEN 'CARNET EXTRANJERIA' WHEN PNAT1.CARNETFUERZASARMADASINPUT IS NOT NULL THEN 'CARNET FUERZAS ARMADAS' WHEN PNAT1.CARNETIDENPOLICIAINPUT IS NOT NULL THEN 'CARNET POLICIA' WHEN PNAT1.CARNETMINISTRABINPUT IS NOT NULL THEN 'CARNET MINISTERIO TRABAJO' WHEN PNAT1.DOCUMENTOIDENTIDADINPUT IS NOT NULL THEN 'DOCUMENTO DE IDENTIDAD' WHEN PNAT1.PARTIDANACIMIENTOINPUT IS NOT NULL THEN 'PARTIDA DE NACIMIENTO' WHEN PNAT1.LIBRETAMILITARINPUT IS NOT NULL THEN 'LIBRETA MILITAR' WHEN PNAT1.RUCNATURALINPUT IS NOT NULL THEN 'RUC NATURAL' ELSE '' END AS TEXT) AS TIPO_DOCUMENTO_CONT, CAST(CASE WHEN PNAT1.PASAPORTEINPUT IS NOT NULL THEN PNAT1.PASAPORTEINPUT WHEN PNAT1.CARNETDIPLOMATICOINPUT IS NOT NULL THEN PNAT1.CARNETDIPLOMATICOINPUT WHEN PNAT1.CARNETEXTRANJERIAINPUT IS NOT NULL THEN PNAT1.CARNETEXTRANJERIAINPUT WHEN PNAT1.CARNETFUERZASARMADASINPUT IS NOT NULL THEN PNAT1.CARNETFUERZASARMADASINPUT WHEN PNAT1.CARNETIDENPOLICIAINPUT IS NOT NULL THEN PNAT1.CARNETIDENPOLICIAINPUT WHEN PNAT1.CARNETMINISTRABINPUT IS NOT NULL THEN PNAT1.CARNETMINISTRABINPUT WHEN PNAT1.DOCUMENTOIDENTIDADINPUT IS NOT NULL THEN PNAT1.DOCUMENTOIDENTIDADINPUT WHEN PNAT1.PARTIDANACIMIENTOINPUT IS NOT NULL THEN PNAT1.PARTIDANACIMIENTOINPUT WHEN PNAT1.LIBRETAMILITARINPUT IS NOT NULL THEN PNAT1.LIBRETAMILITARINPUT WHEN PNAT1.RUCNATURALINPUT IS NOT NULL THEN PNAT1.RUCNATURALINPUT ELSE'' END AS TEXT) AS NUM_DOCUMENTO_CONT, CONCAT(NULLIF(RTRIM(NULLIF(LTRIM(PNAT1.NOMBREINPUT),'')),''),CASE WHEN NULLIF(RTRIM(NULLIF(LTRIM(PNAT1.SEGUNDONOMBREINPUT),'')),'') IS NOT NULL THEN '' ELSE '' END) AS NOMBRE_CONTR, NULLIF(RTRIM(NULLIF(LTRIM(PNAT1.APELLIDOINPUT),'')),'') AS APE_PATCONTR, NULLIF(RTRIM(NULLIF(LTRIM(PNAT1.APELLIDOMATERNOINPUT),'')),'') AS APE_MATCONTR, PRETH2.ACTIVIDADECONOMICAINPUT AS ACTIVIDAD_ECON, PNAT1.NACIONALIDADINPUT AS NACIONALIDAD, 'ASEGURADO' AS TIPO_CLIENTE, 'NATURAL' AS TIPO_PERSONA, TD.DEPARTAMENTO, TD.PROVINCIA, TD.DISTRITO, TD.COD_UBIGEO, PNAT1.FECHANACIMIENTOINPUT AS FECNACIMIENTO, PNAT1.SEXOINPUT AS SEXO FROM TBL_UNI_MASIVOS P1 INNER JOIN PRE_ASEG_MAS POLPAR ON P1.ITEM = POLPAR.ITEM INNER JOIN INTERSEGURO.PERSONANATURAL PNAT1 ON POLPAR.THIRDPARTYID = PNAT1.STATIC INNER JOIN INTERSEGURO.PRETHIRDPARTY PRETH2 ON PRETH2.STATIC = PNAT1.STATIC LEFT JOIN TMP_DIR_TOTAL TD ON TD.TPT_ID = PRETH2.STATIC;",'alloy')
    logger.info(f'poblar_mig_asegurado_mas - fin')
    return res

def poblar_mig_asegurado_mas2():
    logger.info(f'poblar_mig_asegurado_mas2 - inicio')
    res=execute_query_with_results(f"insert into interseguror.MIG_ASEGURADO_MAS SELECT P1.ITEM AS IDPOLIZA, POLC.TIPODOCTRAMAINPUT, POLC.NUMERODOCTRAMAINPUT, CONCAT( NULLIF ( RTRIM( NULLIF ( LTRIM( POLC.NOMBREUNOTRAMAINPUT), '' ) ), '' ), CASE WHEN NULLIF ( RTRIM( NULLIF ( LTRIM( POLC.NOMBREDOSTRAMAINPUT ), '' ) ), '' ) IS NOT NULL THEN '' ELSE'' END ) AS NOMBRE_ASEG, POLC.APELLIDOUNOTRAMAINPUT, POLC.APELLIDODOSTRAMAINPUT, NULL, NULL, 'ASEGURADO' AS TIPO_CLIENTE, 'NATURAL' AS TIPO_PERSONA, NULL, NULL, NULL, NULL, POLC.FNACTRAMAINPUT, NULL FROM TBL_UNI_MASIVOS P1 INNER JOIN INTERSEGURO.POLICYDCO DCO ON DCO.OPERATIONPK = P1. ID INNER JOIN INTERSEGURO.POLDESGPERSONAL POLC ON POLC.PK = DCO.DCOID;",'alloy')
    logger.info(f'poblar_mig_asegurado_mas2 - fin')
    return res

def poblar_mig_asegurado_mas3():
    logger.info(f'poblar_mig_asegurado_mas3 - inicio')
    res=execute_query_with_results(f"insert into interseguror.MIG_ASEGURADO_MAS SELECT P1.ITEM AS IDPOLIZA, POLC.TIPODOCTRAMAINPUT, POLC.NUMERODOCTRAMAINPUT, CONCAT( NULLIF ( RTRIM( NULLIF ( LTRIM( POLC.NOMBREUNOTRAMAINPUT), '' ) ), '' ), CASE WHEN NULLIF ( RTRIM( NULLIF ( LTRIM( POLC.NOMBREDOSTRAMAINPUT ), '' ) ), '' ) IS NOT NULL THEN '' ELSE'' END ) AS NOMBRE_ASEG, POLC.APELLIDOUNOTRAMAINPUT, POLC.APELLIDODOSTRAMAINPUT, NULL, NULL, 'ASEGURADO' AS TIPO_CLIENTE, 'NATURAL' AS TIPO_PERSONA, NULL, NULL, NULL, NULL, POLC.FNACTRAMAINPUT, NULL FROM TBL_UNI_MASIVOS P1 INNER JOIN INTERSEGURO.POLICYDCO DCO ON DCO.OPERATIONPK = P1. ID INNER JOIN INTERSEGURO.POLDESGTARJETAS POLC ON POLC.PK = DCO.DCOID;",'alloy')
    logger.info(f'poblar_mig_asegurado_mas3 - fin')
    return res

def poblar_mig_asegurado_mas4():
    logger.info(f'poblar_mig_asegurado_mas4 - inicio')
    res=execute_query_with_results(f"insert into interseguror.MIG_ASEGURADO_MAS SELECT P1.ITEM AS IDPOLIZA, NULL, NULL, NULLIF( RTRIM( NULLIF ( LTRIM( NOMBREASEGURADOINPUT), '' ) ), '' ) AS NOMBRE_ASEG, NULL, NULL, NULL, NULL, 'ASEGURADO' AS TIPO_CLIENTE, 'NATURAL' AS TIPO_PERSONA, NULL, NULL, NULL, NULL, NULL, NULL FROM TBL_UNI_MASIVOS P1 INNER JOIN INTERSEGURO.POLICYDCO DCO ON DCO.OPERATIONPK = P1. ID INNER JOIN INTERSEGURO.POLCLONACIONOFRAUDED POLC ON POLC.PK = DCO.DCOID",'alloy')
    logger.info(f'poblar_mig_asegurado_mas4 - fin')
    return res

def poblar_mig_asegurado_mas5():
    logger.info(f'poblar_mig_asegurado_mas5 - inicio')
    res=execute_query_with_results(f"insert into interseguror.MIG_ASEGURADO_MAS SELECT P1.ITEM AS IDPOLIZA, POLC.TIPODOCUMENTOGEINPUT, POLC.NUMERODOCUMENTOGEINPUT, NULLIF(RTRIM(NULLIF(LTRIM(POLC.NOMBREUNOASEGINPUT),'')),'') AS NOMBRE_ASEG, NULL, NULL, NULL, NULL, 'ASEGURADO' AS TIPO_CLIENTE, 'NATURAL' AS TIPO_PERSONA, NULL, NULL, NULL, NULL, NULL, NULL FROM TBL_UNI_MASIVOS P1 INNER JOIN INTERSEGURO.POLICYDCO DCO ON DCO.OPERATIONPK = P1.ID INNER JOIN INTERSEGURO.POLGARANTIAEXTENDIDA POLC ON POLC.PK = DCO.DCOID",'alloy')
    logger.info(f'poblar_mig_asegurado_mas5 - fin')
    return res

def poblar_tmp_uni_benef_mas(fecha):
    logger.info(f'poblar_tmp_uni_benef_mas - inicio')
    res=execute_query_with_results(f"insert into interseguror.TMP_UNI_BENEF_MAS SELECT CLM.POLICYID, PO.POR_BENEFICIARY_ID AS TPT_ID, MAX( TUV.ID) AS ID FROM TBL_UNI_MASIVOS TUV INNER JOIN INTERSEGURO.CLAIM CLM ON TUV.ITEM = CLM.POLICYID INNER JOIN INTERSEGURO.CLAIMRISKUNIT CRU ON CRU.CLAIMID = CLM.CLAIMID INNER JOIN INTERSEGURO.CLAIMINSURANCEOBJECT CIO ON CIO.CLAIMRISKUNITID = CRU.CLAIMRISKUNITID INNER JOIN INTERSEGURO.CLAIMNORMALRESERVE CLNR ON CIO.CLAIMINSURANCEOBJECTID = CLNR.CLAIMINSURANCEOBJECTID INNER JOIN INTERSEGURO.PAYMENTORDER PO ON PO.FKRESERVE = CLNR.CLAIMNORMALRESERVEDID WHERE CLM.CLAIMDATE < to_timestamp('{fecha}', 'YYYY-MM-DD') AND CLM.STATE != 2 GROUP BY CLM.POLICYID, PO.POR_BENEFICIARY_ID",'alloy')
    logger.info(f'poblar_tmp_uni_benef_mas - fin')
    return res

def poblar_mig_benef_sin_mas():
    logger.info(f'poblar_mig_benef_sin_mas - inicio')
    res=execute_query_with_results(f"insert into interseguror.MIG_BENEF_SIN_MAS SELECT DISTINCT TUB.POLICYID AS IDPOLIZA, CAST( CASE WHEN PJUR.RUCINPUT IS NOT NULL THEN 'RUC' WHEN PNAT.PASAPORTEINPUT IS NOT NULL THEN 'PASAPORTE' WHEN PNAT.CARNETDIPLOMATICOINPUT IS NOT NULL THEN 'CARNET DIPLOMATICO' WHEN PNAT.CARNETEXTRANJERIAINPUT IS NOT NULL THEN 'CARNET EXTRANJERIA' WHEN PNAT.CARNETFUERZASARMADASINPUT IS NOT NULL THEN 'CARNET FUERZAS ARMADAS' WHEN PNAT.CARNETIDENPOLICIAINPUT IS NOT NULL THEN 'CARNET POLICIA' WHEN PNAT.CARNETMINISTRABINPUT IS NOT NULL THEN 'CARNET MINISTERIO TRABAJO' WHEN PNAT.DOCUMENTOIDENTIDADINPUT IS NOT NULL THEN 'DOCUMENTO DE IDENTIDAD' WHEN PNAT.PARTIDANACIMIENTOINPUT IS NOT NULL THEN 'PARTIDA DE NACIMIENTO' WHEN PNAT.LIBRETAMILITARINPUT IS NOT NULL THEN 'LIBRETA MILITAR' WHEN PNAT.RUCNATURALINPUT IS NOT NULL THEN 'RUC NATURAL' ELSE' ' END AS TEXT) AS TIPODOCUMENTOIDENTIDAD, CAST ( CASE WHEN PJUR.RUCINPUT IS NOT NULL THEN PJUR.RUCINPUT WHEN PNAT.PASAPORTEINPUT IS NOT NULL THEN PNAT.PASAPORTEINPUT WHEN PNAT.CARNETDIPLOMATICOINPUT IS NOT NULL THEN PNAT.CARNETDIPLOMATICOINPUT WHEN PNAT.CARNETEXTRANJERIAINPUT IS NOT NULL THEN PNAT.CARNETEXTRANJERIAINPUT WHEN PNAT.CARNETFUERZASARMADASINPUT IS NOT NULL THEN PNAT.CARNETFUERZASARMADASINPUT WHEN PNAT.CARNETIDENPOLICIAINPUT IS NOT NULL THEN PNAT.CARNETIDENPOLICIAINPUT WHEN PNAT.CARNETMINISTRABINPUT IS NOT NULL THEN PNAT.CARNETMINISTRABINPUT WHEN PNAT.DOCUMENTOIDENTIDADINPUT IS NOT NULL THEN PNAT.DOCUMENTOIDENTIDADINPUT WHEN PNAT.PARTIDANACIMIENTOINPUT IS NOT NULL THEN PNAT.PARTIDANACIMIENTOINPUT WHEN PNAT.LIBRETAMILITARINPUT IS NOT NULL THEN PNAT.LIBRETAMILITARINPUT WHEN PNAT.RUCNATURALINPUT IS NOT NULL THEN PNAT.RUCNATURALINPUT ELSE'' END AS TEXT ) AS NUMERODOCUMENTOIDENTIDAD, COALESCE ( CONCAT ( NULLIF ( RTRIM( NULLIF ( LTRIM( PNAT.NOMBREINPUT ), '' ) ), '' ), CASE WHEN NULLIF ( RTRIM( NULLIF ( LTRIM( PNAT.SEGUNDONOMBREINPUT ), '' ) ), '' ) IS NOT NULL THEN '' ELSE'' END ), PRETH2.ACTIVIDADECONOMICAINPUT ) AS NOMBRE_ASEG, NULLIF ( RTRIM( NULLIF ( LTRIM( PNAT.APELLIDOINPUT ), '' ) ), '' ) AS APE_PATASEG, NULLIF ( RTRIM( NULLIF ( LTRIM( PNAT.APELLIDOMATERNOINPUT ), '' ) ), '' ) AS APE_MATASEG, PRETH2.ACTIVIDADECONOMICAINPUT AS ACTIVIDAD_ECON, PNAT.NACIONALIDADINPUT AS NACIONALIDAD, 'BENEFICIARIO' AS TIPO_CLIENTE, CASE WHEN PNAT.STATIC IS NOT NULL THEN 'NATURAL' ELSE'JURIDICO' END AS TIPO_PERSONA, TD.DEPARTAMENTO, TD.PROVINCIA, TD.DISTRITO, TD.COD_UBIGEO, COALESCE ( PNAT.FECHANACIMIENTOINPUT, PJUR.FECHACONSTITUCIONINPUT ) AS FECNACIMIENTO, PNAT.SEXOINPUT AS SEXO FROM TMP_UNI_BENEF_MAS TUB INNER JOIN INTERSEGURO.PRETHIRDPARTY PRETH2 ON PRETH2.STATIC = TUB.TPT_ID LEFT JOIN INTERSEGURO.PERSONANATURAL PNAT ON PNAT.STATIC = TUB.TPT_ID LEFT JOIN INTERSEGURO.PERSONAJURIDICA PJUR ON PJUR.STATIC = TUB.TPT_ID LEFT JOIN TMP_DIR_TOTAL TD ON TD.TPT_ID = TUB.TPT_ID",'alloy')
    logger.info(f'poblar_mig_benef_sin_mas - fin')
    return res

def poblar_tmp_montos(fecha,fecha_inicio_recaudacion,fecha_corte_fin):
    logger.info(f'poblar_tmp_montos - inicio')
    query1=f"insert into interseguror.TMP_OP_MASIVOS SELECT TUV.ITEM,max(OP.OPENITEMID) AS OPENITEMID FROM TBL_UNI_MASIVOS TUV INNER JOIN INTERSEGURO.OPENITEMREFERENCE OPR ON OPR.POLICYID = TUV.ITEM INNER JOIN INTERSEGURO.OPENITEM OP ON OP.OPENITEMID = OPR.OPENITEMID WHERE OP.DTY_ID = 7572 AND OP.STATUS IN('active','applied') group by TUV.ITEM"
    query2=f"insert into interseguror.TMP_OP_MASIVOS_MONTO SELECT TOV.ITEM ,OP.OPENITEMID,OP.AMOUNT FROM TMP_OP_MASIVOS TOV INNER JOIN INTERSEGURO.OPENITEM OP ON OP.OPENITEMID = TOV.OPENITEMID"
    query3=f"insert into interseguror.TMP_OP_PH_MASIVOS SELECT TUV.ITEM,max(OP.OPENITEMID) AS OPENITEMID FROM TBL_UNI_MASIVOS TUV INNER JOIN INTERSEGURO.OPENITEMREFERENCE OPR ON OPR.POLICYID = TUV.ITEM INNER JOIN INTERSEGURO.OPENITEM OP ON OP.OPENITEMID = OPR.OPENITEMID WHERE OP.DTY_ID = 7572 AND OP.STATUS IN('applied') AND OP.DOCDATE < to_timestamp('{fecha}','YYYY-MM-DD') group by TUV.ITEM"
    query4=f"insert into interseguror.TMP_OP_MASIVOS_PH_MONTO SELECT TOV.ITEM ,OP.OPENITEMID,OP.AMOUNT,OP.DOCDATE,OP.DATEUSERECIPENT FROM TMP_OP_PH_MASIVOS TOV INNER JOIN INTERSEGURO.OPENITEM OP ON OP.OPENITEMID = TOV.OPENITEMID"
    query5=f"insert into interseguror.TMP_OP_PH_MASIVOS_V2 SELECT TUV.ITEM,max(OP.OPENITEMID) AS OPENITEMID FROM TBL_UNI_MASIVOS TUV INNER JOIN INTERSEGURO.OPENITEMREFERENCE OPR ON OPR.POLICYID = TUV.ITEM INNER JOIN INTERSEGURO.OPENITEM OP ON OP.OPENITEMID = OPR.OPENITEMID WHERE OP.DTY_ID = 7572 AND OP.STATUS IN('active') AND OP.DOCDATE < to_timestamp('{fecha}','YYYY-MM-DD') group by TUV.ITEM"
    query6=f"delete from interseguror.TMP_OP_PH_MASIVOS_V2 where ITEM in(select ITEM from interseguror.TMP_OP_PH_MASIVOS)"
    query7=f"insert into interseguror.TMP_OP_MASIVOS_PH_MONTO_V2 SELECT TOV.ITEM ,OP.OPENITEMID,OP.AMOUNT,OP.DOCDATE,OP.DATEUSERECIPENT,OP.STATUS FROM TMP_OP_PH_MASIVOS_V2 TOV INNER JOIN INTERSEGURO.OPENITEM OP ON OP.OPENITEMID = TOV.OPENITEMID"
    query8=f"insert into interseguror.TMP_OP_MASIVOS_RECAUDADO SELECT TUV.ITEM,sum(OP.AMOUNT) AS MONTO_PRIMA FROM TBL_UNI_MASIVOS TUV INNER JOIN INTERSEGURO.OPENITEMREFERENCE OPR ON OPR.POLICYID = TUV.ITEM INNER JOIN INTERSEGURO.OPENITEM OP ON OP.OPENITEMID = OPR.OPENITEMID WHERE OP.DTY_ID = 7492 AND OP.DOCDATE >= to_timestamp('{fecha_inicio_recaudacion}','YYYY-MM-DD') AND OP.DOCDATE <= to_timestamp('{fecha_corte_fin}','YYYY-MM-DD') group by TUV.ITEM"
    res=execute_query_with_results(query1,'alloy')
    res=execute_query_with_results(query2,'alloy')
    res=execute_query_with_results(query3,'alloy')
    res=execute_query_with_results(query4,'alloy')
    res=execute_query_with_results(query5,'alloy')
    res=execute_query_with_results(query6,'alloy')
    res=execute_query_with_results(query7,'alloy')
    res=execute_query_with_results(query8,'alloy')
    logger.info(f'poblar_tmp_montos - fin')
    return res

def querys_filtros(fecha):
    logger.info(f'querys_filtros - inicio')
    query1=f"UPDATE TBL_UNI_MASIVOS TUM SET FLAG_CARGA = 0 WHERE TUM.PRODUCTO IN('DesgravamenHipotecario', 'DesgravamenVehicular', 'GarantiaExtendida', 'SCTR'); "
    query2=f"UPDATE TBL_UNI_MASIVOS TUM SET FLAG_CARGA = 0 WHERE TUM.PRODUCTO IN('DesgHipotecarioIndividual', 'DesgPersonalIndividual', 'DesgravamenPersonal', 'DesgravamenTarjetas', 'DesgVehicularIndividual') AND to_timestamp(TUM.FECHACARGASISTEMAINPUT, 'YYYY-MM-DD') > (to_timestamp('{fecha}', 'YYYY-MM-DD') + INTERVAL '-1 day')::date; "
    query3=f"UPDATE TBL_UNI_MASIVOS SET FLAG_CARGA = 0 FROM( SELECT A.ITEM FROM TMP_OP_MASIVOS_PH_MONTO A INNER JOIN TBL_UNI_MASIVOS B ON A.ITEM = B.ITEM WHERE B.PRODUCTO IN ('DesgHipotecarioIndividual', 'DesgPersonalIndividual', 'DesgVehicularIndividual') AND A.DATEUSERECIPENT + INTERVAL '3 month' < (to_timestamp('{fecha}', 'YYYY-MM-DD') + INTERVAL '-1 day')::date) ICB WHERE TBL_UNI_MASIVOS.ITEM = ICB.ITEM;"
    query4=f"UPDATE TBL_UNI_MASIVOS SET FLAG_CARGA = 0 FROM( SELECT A.ITEM FROM TMP_OP_MASIVOS_PH_MONTO_V2 A INNER JOIN TBL_UNI_MASIVOS B ON A.ITEM = B.ITEM WHERE B.PRODUCTO IN ('DesgHipotecarioIndividual', 'DesgPersonalIndividual', 'DesgVehicularIndividual') AND A.DATEUSERECIPENT + INTERVAL '3 month' < (to_timestamp('{fecha}', 'YYYY-MM-DD') + INTERVAL '-1 day')::date) ICB WHERE TBL_UNI_MASIVOS.ITEM = ICB.ITEM;"
    query5=f"UPDATE TBL_UNI_MASIVOS SET FLAG_CARGA = 0 FROM( SELECT A.ITEM FROM TMP_OP_MASIVOS_PH_MONTO A INNER JOIN TBL_UNI_MASIVOS B ON A.ITEM = B.ITEM WHERE B.PRODUCTO IN ('DesgravamenPersonal', 'DesgravamenTarjetas') AND A.DATEUSERECIPENT < (to_timestamp('{fecha}', 'YYYY-MM-DD') + INTERVAL '-1 day')::date) ICB WHERE TBL_UNI_MASIVOS.ITEM = ICB.ITEM;"
    query6=f"UPDATE TBL_UNI_MASIVOS SET FLAG_CARGA = 0 FROM( SELECT A.ITEM FROM TMP_OP_MASIVOS_PH_MONTO_V2 A INNER JOIN TBL_UNI_MASIVOS B ON A.ITEM = B.ITEM WHERE B.PRODUCTO IN ('DesgravamenPersonal', 'DesgravamenTarjetas') AND A.DATEUSERECIPENT < (to_timestamp('{fecha}', 'YYYY-MM-DD') + INTERVAL '-1 day')::date) ICB WHERE TBL_UNI_MASIVOS.ITEM = ICB.ITEM;"
    query7=f"UPDATE TBL_UNI_MASIVOS TUM SET FLAG_CARGA = 0 WHERE TUM.PRODUCTO IN( 'AccidenteAsistenciaOH', 'Extracash', 'ExtracashFuno', 'ExtracashSCC', 'InterbankAccidentes', 'ProtBlindajeIndividualPlus', 'ProtBlindajeIndiviPlusFuno', 'ProteccionBlindaje', 'ProteccionBlindajePlus', 'ProteccionCreditos', 'ProteccionCreditosSCC', 'ProteccionDebito', 'ProteccionFamiliar', 'PTIndividualIBK', 'TarjetaDeCredito', 'TarjetaDeDebito', 'VeaAccidentes', 'VeaVida') AND TUM.FINISHDATE < (to_timestamp('{fecha}', 'YYYY-MM-DD') + INTERVAL '-1 day')::date; "
    query8=f"UPDATE TBL_UNI_MASIVOS SET FLAG_CARGA = 0 FROM( SELECT A.ITEM FROM TMP_OP_MASIVOS_PH_MONTO A INNER JOIN TBL_UNI_MASIVOS B ON A.ITEM = B.ITEM WHERE B.PRODUCTO IN ('VidaGrupoComplementario', 'VidaLeyTTL') AND A.DATEUSERECIPENT + INTERVAL '3 month' < (to_timestamp('{fecha}', 'YYYY-MM-DD') + INTERVAL '-1 day')::date) ICB WHERE TBL_UNI_MASIVOS.ITEM = ICB.ITEM;"
    query9=f"UPDATE TBL_UNI_MASIVOS SET FLAG_CARGA = 0 FROM( SELECT A.ITEM FROM TMP_OP_MASIVOS_PH_MONTO_V2 A INNER JOIN TBL_UNI_MASIVOS B ON A.ITEM = B.ITEM WHERE B.PRODUCTO IN ('VidaGrupoComplementario', 'VidaLeyTTL') AND A.DATEUSERECIPENT + INTERVAL '3 month' < (to_timestamp('{fecha}', 'YYYY-MM-DD') + INTERVAL '-1 day')::date) ICB WHERE TBL_UNI_MASIVOS.ITEM = ICB.ITEM;"
    query10=f"UPDATE TBL_UNI_MASIVOS TUM SET FLAG_CARGA = 0 WHERE TUM.PRODUCTO IN('SOAT') AND TUM.FINISHDATE < (to_timestamp('{fecha}', 'YYYY-MM-DD') + INTERVAL '-1 day')::date;"
    query11=f"UPDATE TBL_UNI_MASIVOS TUM SET FLAG_CARGA = 0 WHERE TUM.PRODUCTO IN('VidaLeyTTL', 'VidaGrupoComplementario', 'ClonacionOFraude'); "
    query12=f"UPDATE TBL_UNI_MASIVOS SET FLAG_CARGA = 0 FROM( SELECT DISTINCT TUV.ITEM FROM TBL_UNI_MASIVOS TUV LEFT JOIN TBL_PERSONAS_MASIVOS A1 ON A1.IDPOLIZA = TUV.ITEM WHERE TUV.PRODUCTO IN ('DesgPersonalIndividual', 'DesgHipotecarioIndividual') AND UPPER(A1.TIPO_CLIENTE) = 'BENEFICIARIO') DB1 WHERE TBL_UNI_MASIVOS.ITEM = DB1.ITEM;"
    res=execute_query_with_results(query1,'alloy')
    res=execute_query_with_results(query2,'alloy')
    res=execute_query_with_results(query3,'alloy')
    res=execute_query_with_results(query4,'alloy')
    res=execute_query_with_results(query5,'alloy')
    res=execute_query_with_results(query6,'alloy')
    res=execute_query_with_results(query7,'alloy')
    res=execute_query_with_results(query8,'alloy')
    res=execute_query_with_results(query9,'alloy')
    res=execute_query_with_results(query10,'alloy')
    res=execute_query_with_results(query11,'alloy')
    res=execute_query_with_results(query12,'alloy')
    logger.info(f'querys_filtros - fin')
    return res

def poblar_productos():
    logger.info(f'poblar_productos - inicio')
    query1=f"INSERT INTO TMP_REP_CLASS_PRPODUCT SELECT V.PRODUCTID, NULL, V.CODIGOSBSINPUT, NULL FROM INTERSEGURO.VIEW_PRODUCTOS_SAMP V;"
    query2=f"UPDATE TMP_REP_CLASS_PRPODUCT T SET CLASE = 'Vida' WHERE T.PRODUCTID IN( 69343, 69344, 69345, 58757, 69346, 69347, 69348, 69349, 52823, 52824, 58758, 64899, 69350, 52820, 52821, 69352, 69351, 58759, 58760, 69353, 58761, 69354, 69355, 52819, 52822, 77829, 77830, 77831, 77909);"
    query3=f"UPDATE TMP_REP_CLASS_PRPODUCT T SET CLASE = 'Colectivo' WHERE T.PRODUCTID IN( 27277, 39482, 35442, 35450, 35447, 35445, 31401, 56757, 31403, 39480, 39479, 35440, 35441, 35452, 39481, 24877, 28837, 69141, 69545, 26437, 24917, 73787, 35451, 29637, 30437, 27237, 28037, 31399, 31402, 35439, 31404, 77889, 77890, 77929);"
    query4=f"UPDATE TMP_REP_CLASS_PRPODUCT T SET CLASE = 'Masivo' WHERE T.PRODUCTID IN( 60859, 61263, 73585, 61061, 44739, 48779, 68939, 69747, 73989, 35448, 77827, 77828, 77869, 77849);"
    res=execute_query_with_results(query1,'alloy')
    res=execute_query_with_results(query2,'alloy')
    res=execute_query_with_results(query3,'alloy')
    res=execute_query_with_results(query4,'alloy')
    logger.info(f'poblar_productos - fin')
    return res

def poblar_final_masivos():
    logger.info(f'poblar_final_masivos - inicio')
    query1=f"INSERT INTO INTERSEGUROR.RPT_FINAL_MASIVOS SELECT PREP.NUMEROPOLIZAINPUT AS NUM_POLIZA, TCLASS.CLASE AS LINEA_NEGOCIO, PREP.MONEDAINPUT AS MONEDA, TOV.AMOUNT AS PRIMA, PREP.FRECUENCIAPAGOINPUT AS FRECUENCIA_PAGO, A1.COD_UBIGEO AS COD_UBIGEO, A1.DEPARTAMENTO AS DEPARTAMENTO, A1.PROVINCIA AS PROVINCIA, A1.DISTRITO AS DISTRITO, A1.APE_PATCONTR AS APELLIDO_PATERNO, A1.APE_MATCONTR AS APELLIDO_MATERNO, CASE A1.TIPO_PERSONA WHEN 'NATURAL' THEN A1.NOMBRE_CONTR ELSE'' END AS NOMBRE, CASE A1.TIPO_PERSONA WHEN 'JURIDICO' THEN A1.NOMBRE_CONTR ELSE'' END AS RAZON_SOCIAL, A1.TIPO_DOCUMENTO_CONT AS TIPO_DOCUMENTO, A1.NUM_DOCUMENTO_CONT AS NRO_DOCUMENTO, A1.TIPO_CLIENTE AS TIPO_CLIENTE, A1.NACIONALIDAD AS NACIONALIDAD, A1.TIPO_PERSONA AS TIPO_PERSONA, PRO.DESCRIPTION AS GLOSA_PRODUCTO, PREPRO.CODIGOPRODUCTOVALUE AS CODIGO_PRODUCTO, PREPRO.RAMOVALUE AS CODIGO_RAMO, PREPRO.SUBRAMOVALUE AS CODIGO_SUBRAMO, PREPRO.CODIGOSBSINPUT AS COD_RAMO_SBS, A1.ACTIVIDAD_ECON AS ACTIVIDAD_ECONOMICA, PDCO.INITIALDATE AS FCH_INICIO_VIGENCIA, PDCO.FINISHDATE AS FCH_FIN_VIGENCIA, PREP.FECHAEMISIONINPUT AS FCH_EMISION, B1.DATEUSERECIPENT AS FEC_PAGADOHASTA, TUV.FLAG_CARGA, A1.FECNACIMIENTO, '' AS COD_ACTI_ECON, REC.MONTO_PRIMA AS PRIMA_RECAUDADA FROM TBL_UNI_MASIVOS TUV INNER JOIN TMP_OP_MASIVOS_MONTO TOV ON TUV.ITEM = TOV.ITEM INNER JOIN INTERSEGURO.POLICYDCO PDCO ON TUV.ID = PDCO.OPERATIONPK INNER JOIN INTERSEGURO.PREPOLICY PREP ON PREP.PK = PDCO.DCOID INNER JOIN INTERSEGURO.AGREGATEDPOLICY AP ON PREP.STATIC = AP.AGREGATEDPOLICYID INNER JOIN INTERSEGURO.PRODUCT PRO ON PRO.PRODUCTID = AP.PRODUCTID INNER JOIN INTERSEGURO.PRODUCTPROPERTY PRE ON PRE.PRO_ID = PRO.PRODUCTID INNER JOIN INTERSEGURO.PREPRODUCT PREPRO ON PREPRO.STATIC = PRO.PRODUCTID INNER JOIN TMP_REP_CLASS_PRPODUCT TCLASS ON TCLASS.PRODUCTID = PRO.PRODUCTID LEFT JOIN TBL_PERSONAS_MASIVOS A1 ON A1.IDPOLIZA = TUV.ITEM LEFT JOIN TMP_OP_MASIVOS_PH_MONTO B1 ON B1.ITEM = TUV.ITEM LEFT JOIN TMP_OP_MASIVOS_RECAUDADO REC ON REC.ITEM = TUV.ITEM;"
    res=execute_query_with_results(query1,'alloy')
    logger.info(f'poblar_final_masivos - fin')
    return res  

def actualizar_actividad_economica():
    logger.info(f'actualizar_actividad_economica - inicio')
    query1=f"UPDATE RPT_FINAL_MASIVOS SET COD_ACTI_ECON = ICB.CODIGO FROM( SELECT MAX ( TRANSFORMADORFILAID) AS CODIGO, DESCRIPTION AS DESCRIPCION FROM INTERSEGURO.TRANSFORMADORFILA WHERE PROPERTYID = 3706454 GROUP BY DESCRIPTION ) ICB WHERE RPT_FINAL_MASIVOS.ACTIVIDAD_ECONOMICA = ICB.DESCRIPCION;"
    res=execute_query_with_results(query1,'alloy')
    logger.info(f'actualizar_actividad_economica - fin')
    return res  

def poblar_plaft_transaccional():
    logger.info(f'poblar_plaft_transaccional - inicio')
    query1=f"INSERT INTO interseguror.PLAFT_TRANSACCIONAL( ID_REP_GENERAL, NUMERO_POLIZA, LINEA_NEGOCIO, COD_MONEDA, MONTO_PRIMA, COD_FRECUENCIA_PAGO, MONTO_PRIMA_TOTAL, NOMBRE_RAZON_SOCIAL, APE_PATERNO, APE_MATERNO, COD_TIPO_DOCUMENTO, NUMERO_DOCUMENTO, TIPO_CLIENTE, NACIONALIDAD, TIPO_PERSONA, COD_RAMO, COD_SUBRAMO, COD_PRODUCTO, COD_PRODUCTO_SBS, COD_ACTIVIDAD_ECONOMICA, ACTIVIDAD_ECONOMICA, ID_REGIMEN, FEC_EMISION_POLIZA, FEC_INICIO_VIGENCIA, FEC_FIN_VIGENCIA, ORIGEN, DEPARTAMENTO, NACIONALIDAD_EVAL, DEPARTAMENTO_EVAL, ID_DEPARTAMENTO, GLOSA_PRODUCTO, MONTO_PRIMA_RECAUDADA, FECHA_NACIMIENTO) SELECT NULL, R.NUM_POLIZA, R.LINEA_NEGOCIO, EM.CODIGO_EQUIVALENTE AS COD_MONEDA, R.PRIMA AS MONTO_PRIMA, EF.CODIGO_EQUIVALENTE AS FRECUENCIA_PAGO, R.PRIMA AS MONTO_TOTAL_PRIMA, CASE WHEN TIPO_PERSONA = 'JURIDICO' THEN R.RAZON_SOCIAL ELSE R.NOMBRE END AS NOMBRE_RAZON_SOCIAL, R.APELLIDO_PATERNO, R.APELLIDO_MATERNO, ETD.CODIGO_EQUIVALENTE AS COD_TIPO_DOCUMENTO, R.NRO_DOCUMENTO AS NUMERO_DOCUMENTO, R.TIPO_CLIENTE, R.NACIONALIDAD, R.TIPO_PERSONA, R.CODIGO_RAMO, R.CODIGO_SUBRAMO, R.CODIGO_PRODUCTO, R.COD_RAMO_SBS AS COD_PRODUCTO_SBS, R.COD_ACTI_ECON AS COD_ACTIVIDAD_ECONOMICA, R.ACTIVIDAD_ECONOMICA, NULL AS ID_REGIMEN, to_timestamp( R.FCH_EMISION, 'yyyy-mm-dd' ) AS FEC_EMISION_POLIZA, R.FCH_INICIO_VIGENCIA AS FEC_INICIO_VIGENCIA, R.FCH_FIN_VIGENCIA AS FEC_FIN_VIGENCIA, 'ACSELE' AS ORIGEN, R.DEPARTAMENTO, NULL AS NACIONALIDAD_EVAL, NULL AS DEPARTAMENTO_EVAL, NULL AS ID_DEPARTAMENTO, R.GLOSA_PRODUCTO, R.PRIMA_RECAUDADA, to_timestamp( R.FECNACIMIENTO, 'yyyy-mm-dd' ) AS FECHA_NACIMIENTO FROM RPT_FINAL_MASIVOS R LEFT JOIN PLAFT_EQUIVALENCIA EF ON ( UPPER ( R.FRECUENCIA_PAGO ) = UPPER ( EF.CODIGO_ORIGEN ) AND EF.ORIGEN = 'ACSELE' AND EF.TIPO = 'FRECUENCIAPAGO' ) LEFT JOIN PLAFT_EQUIVALENCIA EM ON ( UPPER ( R.MONEDA ) = UPPER ( EM.CODIGO_ORIGEN ) AND EM.ORIGEN = 'ACSELE' AND EM.TIPO = 'MONEDA' ) LEFT JOIN PLAFT_EQUIVALENCIA ETD ON ( UPPER ( R.TIPO_DOCUMENTO ) = UPPER ( ETD.CODIGO_ORIGEN ) AND ETD.ORIGEN = 'ACSELE' AND ETD.TIPO = 'TIPODOCUMENTO' ) WHERE R.FLAG_CARGA = 1; "
    res=execute_query_with_results(query1,'alloy')
    logger.info(f'poblar_plaft_transaccional - fin')
    return res  
