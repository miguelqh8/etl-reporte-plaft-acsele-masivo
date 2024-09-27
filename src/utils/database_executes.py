from sqlalchemy import text
import pandas as pd
from sqlalchemy.orm import sessionmaker
from io import StringIO
from .database import engines
from .logger import logger
from sqlalchemy.exc import SQLAlchemyError


def execute_query_to_df(query, bind):
    engine = engines[bind]
    try:
        logger.info(f'Ejecutando query => {query}')
        df = pd.read_sql(query, engine)
        return df
    except SQLAlchemyError as e:
        logger.error(f"Error al ejecutar la consulta: {str(e)}")
        raise 

def execute_query_no_results(query, bind):
    logger.info(f'Ejecutando query => {query}')
    engine = engines[bind]
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as error:
        logger.error(f"Error ejecutando la consulta: {error}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        
def execute_query_with_results(query, bind):
    logger.info(f'Ejecutando query => {query}')
    engine = engines[bind]
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        # Intenta obtener los resultados si es una consulta SELECT
        try:
            results = cursor.fetchall()
        except Exception as e:
            # Si fetchall() falla (por ejemplo, en un UPDATE), retorna el conteo de filas afectadas
            results = cursor.rowcount
            logger.info(f'Filas afectadas => {results}')
        conn.commit()
        return results
    except Exception as error:
        logger.error(f"Error ejecutando la consulta: {error}")
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()

def bulk_insert_from_df(df, table, schema, columns, bind, separador=","):
    logger.info(f'Insertando en tabla  => {schema}.{table}')
    
    # Selecciona solo las columnas que se deben insertar
    df = df[list(columns)]
    
    # Crear un buffer en memoria para almacenar los datos como CSV
    buffer = StringIO()
    
    # Convertir el DataFrame a CSV usando ',' como delimitador y asegurando que los valores con comas estén entrecomillados
    df.to_csv(buffer, index=False, header=False, sep=separador)
    
    # Reiniciar el buffer para que esté listo para leer
    buffer.seek(0)

    # Verificar el contenido del buffer (opcional para depuración)
    #print(buffer.getvalue())  # Esto mostrará los datos del CSV en la consola para verificar su formato
    
    engine = engines[bind]
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        # Establecer el esquema correcto en PostgreSQL
        cursor.execute(f"SET search_path TO {schema}")
        
        # Usar ',' como delimitador en el comando COPY
        cursor.copy_from(buffer, table, sep=separador, columns=columns)
        
        # Confirmar los cambios
        conn.commit()
    except Exception as error:
        logger.error(f"Error durante bulk insert: {error}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
    return True

def bulk_insert_from_df_mssql(df, table, schema, columns, bind):
    logger.info(f'Insertando en tabla => {schema}.{table}')    
    df = df[columns]    
    engine = engines[bind]    
    try:
        df.to_sql(name=table, schema=schema, con=engine, if_exists='append', index=False, method='multi')
    except Exception as error:
        logger.error(f'Error durante bulk insert: {error}')
        return False
    finally:
        engine.dispose()
    return True
