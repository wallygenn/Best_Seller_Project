"""
Objetivo: Disponibilizar información acerca de los vendedores ("seller"),
          mediante la creación de una base que se cargará y se utilizará como base de consulta.
          A fin de de disponibilizar dicha información se utilizaran las apis suministradas.

Funciones creadas: Principal: (persist_seller_data_to_db) - secundarias: (getdb(),get_api_data(...), persist_to_db(...),exists_seller_on_db(...),persist_process_timing_to_db(...))
Tablas usadas: meli.best_seller_tbl, meli.process_timing
"""

import requests
import json
import pymysql
import time
from datetime import datetime
# from datetime import timedelta # para probar con mas días
# Toda esta parte harcodeada debería venir desde alguna parte, file o por alguna llamada donde se le pasa estos valores
product = "Samsung Galaxy A20"
limit = 50
search_api_url = "https://api.mercadolibre.com/sites/MLB/search?q="
item_api_url = "https://api.mercadolibre.com/items/"
currency_api_url = "https://api.mercadolibre.com/currency_conversions/search?from="
# Variable globales, estas se usan para tomar metricas del proceso
api_search_time = ""
api_item_time = ""
api_curr_conversions_time = ""
db_inserted_rows = 0
db_time = 0
# Tomo la fecha del sistema en formato datetime
# now = datetime.now() + timedelta(days=1, hours=3) # para probar con mas días
now = datetime.now()
today = now.strftime("%d/%m/%Y %H:%M:%S")
date_today = now.strftime("%d/%m/%Y")

def getdb():
    connect_db_str = {
    'user': "guest",
    'password': "guest",
    'host': "localhost",
    'database': "meli"
    }
    db = pymysql.connect(**connect_db_str)
    return db

# Esta función está hecha para tomar las url de cualquiera de las tres api y devuelve su correspondiente jason
def get_api_data(url, limit, product, api_name, currency_id, item_id):
    if  api_name == "SEARCH":
        url = url + product + '&limit=' + str(limit)
        response = requests.get(url)
        global api_search_time
        api_search_time = response.elapsed.total_seconds()
    else:
        if  api_name == "ITEM":
            url = url + str(item_id)
            response = requests.get(url)
            global api_item_time
            api_item_time = response.elapsed.total_seconds()
        else:
            if  api_name == "CURRENCY":
                url = url + currency_id + '&to=USD'
                response = requests.get(url)
                global api_curr_conversions_time
                api_curr_conversions_time = response.elapsed.total_seconds()
    data = response.text
    parsed = json.loads(data)
    return parsed

# Esta función se encarga de ejecutar el dml de carga en la base de datos, abre y cierra las conexiones a  la base.
# Además, devuelve cuantos registros fueron cargados.

def persist_to_db (sqlstr):
    db = getdb()
    cursor = db.cursor()
    cursor.execute(sqlstr)
    db.commit()
    affected_rows = db.affected_rows()
    cursor.close()
    db.close()
    return affected_rows

# Esta función revisa si el seller y el producto no fueron ya cargados anteriormente. Retorna 0 si no existe y 1 si ya existe
def exists_seller_on_db (id,seller_id,process_dt):
    db = getdb()
    cursor = db.cursor()
    sql_exists = "select 1 from meli.best_seller_tbl where item_id='" + str(id) + "' AND seller_id='" + str(seller_id) + "'" + " AND DATE_FORMAT(process_dt,'%d/%m/%Y')='" + str(process_dt) + "'"
    exists = cursor.execute(sql_exists)
    cursor.close()
    db.close()
    return exists

# Esta función carga los tiempos de los procesos en la base de datos
def persist_process_timing_to_db(api_time, api_name, db_time, complete_process_time, db_inserted_rows):
    sql_dml = "INSERT INTO MELI.PROCESS_TIMING (SYS_DT, API_TIME, API_NAME, DB_TIME,COMPLETE_PROC_TIME,PROCESS_ROWS) VALUES ( " + "STR_TO_DATE('" + str(today) + "','%d/%m/%Y %H:%i:%S')" + ", '" + str(api_time) + "', '" + str(api_name) + "', '" + str(db_time) + "', '" + str(complete_process_time) + "', '" + str(db_inserted_rows) + "')"
    persist_to_db(sql_dml)

# Esta es la función principal, la misma invoca a las demás con el objetivo de cargar los datos de los seller, los produtos y precios en la Base de datos.
def persist_seller_data_to_db(product,limit,search_api_url, item_api_url, currency_api_url):
    # 1 Recorre apis y carga las variables
    search_api_data = get_api_data(str(search_api_url), str(limit), product.replace(' ', '%20'),"SEARCH",' ',' ')
    for search_api_data_cursor in search_api_data["results"]:
        id = search_api_data_cursor["id"]
        sellerlist = search_api_data_cursor["seller"]
        seller_id = sellerlist["id"]
        price = search_api_data_cursor["price"]
        currency_id = search_api_data_cursor["currency_id"]
        sold_quantity = search_api_data_cursor["sold_quantity"]
        item_api_data = get_api_data(item_api_url, ' ', ' ', 'ITEM', ' ', id)
        shipping = item_api_data["shipping"].get("mode")
        for item_api_data_cursor in item_api_data["sale_terms"]:
            warranty_type = item_api_data_cursor["id"]
            warranty_value = item_api_data_cursor["value_name"]
            if warranty_type == "WARRANTY_TYPE":
                warranty = warranty_value
            else:
                warranty = None
        curr_to_usd_api_data = get_api_data(currency_api_url,' ', ' ','CURRENCY', currency_id, ' ')
        ratio = curr_to_usd_api_data["ratio"]
        # Arma la sentencia DML Insert y ejecuta la carga en la base
        list_fields = "('" + str(id) + "', '" + str(seller_id) + "', '" + str(sold_quantity) + "', '" + str(price) + "', '" + str(currency_id) + "'," + str(ratio) + ", '" + str(warranty) + "', '" + str(shipping) + "', " + "STR_TO_DATE('" + str(date_today) + "','%d/%m/%Y')" + ")"
        inst_dml = "INSERT INTO meli.best_seller_tbl (item_id, seller_id, sales_qty, price, currency, ratio, warranty, shipping, process_dt) VALUES " + list_fields
        if exists_seller_on_db(id, seller_id, date_today):
            print("Seller id: " + str(seller_id) + " and Item id: " + str(id) + " was loaded previously")
        else:
            ini_exec_time = time.time()
            affected_rows = persist_to_db(inst_dml)
            end_exec_time = time.time()
            global db_time
            db_time = db_time + (end_exec_time-ini_exec_time)
            global db_inserted_rows
            db_inserted_rows = db_inserted_rows + affected_rows

# Aca sería la llamada a la función principal
start_time = time.time()
persist_seller_data_to_db(product,limit,search_api_url, item_api_url, currency_api_url)
complete_process_time = time.time() - start_time
# Cargo las metricas del proceso tiempos de proceso
persist_process_timing_to_db(api_search_time, 'SEARCH', db_time, complete_process_time, db_inserted_rows)
persist_process_timing_to_db(api_item_time, 'ITEM', db_time,complete_process_time, db_inserted_rows)
persist_process_timing_to_db(api_curr_conversions_time, 'CURRENCY', db_time, complete_process_time, db_inserted_rows)
