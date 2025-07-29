import json
from json import JSONDecodeError
from jsonschema import validate, ValidationError
from jsonschema.exceptions import SchemaError
import os
import psycopg2
from psycopg2 import OperationalError
import argparse
import yaml
import colorama
from colorama import Fore, Style

colorama.init()


def get_constants(file_name: str) -> list:
    with open(file_name, "r", encoding="utf-8") as file:
        constants = yaml.safe_load(file)
        return constants


constants = get_constants("constants.yaml")

MAIN_SCHEMAS_FOLDER_PATH = constants["MAIN_SCHEMAS_FOLDER_PATH"]
TABLE_SCHEMA_DICT = constants["TABLE_SCHEMA_DICT"]


def get_connection_data() -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--connection_string', required=True)
    args = parser.parse_args()

    connection_string = args.connection_string
    return connection_string


def connect_to_db(connection_data: str) -> "объект типа connection":
    try:
        connection = psycopg2.connect(
            connection_data,
            sslmode="require",
        )
        print("Успешное подключение к базе данных")
        return connection
    except OperationalError as ex:
        print(f"Не удалось подключиться к базе данных. Ошибка: {ex}")


def get_cursor(connection: "объект типа connection") -> "объект типа cursor":
    cursor = connection.cursor()
    return cursor


connection = connect_to_db(get_connection_data())
cursor = get_cursor(connection)


def execute_query(cursor: "объект типа cursor", query: str):
    cursor.execute(query)
    data = cursor.fetchall()
    return data


def get_files_from_folder(path: str) -> list:
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    return files


all_schemas_files = get_files_from_folder(MAIN_SCHEMAS_FOLDER_PATH)


def record_errors_logs(error_log: str) -> None:
    file_path = fr"{'\\'.join(MAIN_SCHEMAS_FOLDER_PATH.split("\\")[:-1])}" + r"\\Errors_Logs.txt"
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(error_log)


def validate_all_json_fields(cursor: "объект типа cursor") -> None:
    invalid_data_quantity = 0
    valid_data_quantity = 0
    all_data_quantity = 0
    for index in range(len(all_schemas_files)):
        key = '.'.join(all_schemas_files[index].split(".")[:-1])
        table_name = '.'.join(TABLE_SCHEMA_DICT[key].split(".")[:-1])
        field_name = TABLE_SCHEMA_DICT[key].split(".")[-1]
        query_for_rows = f"SELECT {field_name}::text FROM {table_name}"
        row = execute_query(cursor, query_for_rows)

        query_for_ids = f"SELECT id FROM {table_name}"
        ids = execute_query(cursor, query_for_ids)

        schema = open(
            f"{MAIN_SCHEMAS_FOLDER_PATH}\\{all_schemas_files[index]}",
            "r",
            encoding="utf-8"
        )

        try:
            schema = json.load(schema)
        except JSONDecodeError as ex:
            print(Fore.RED + f"Ошибка при десериализации json-схемы: {ex.msg} {ex.doc}" + Style.RESET_ALL)

        for i in range(len(row)):
            all_data_quantity += 1
            if row[i][0] is not None:
                json_data = json.loads(row[i][0])
                try:
                    validate(instance=json_data, schema=schema)
                    print("id: ", ids[i][0], table_name + "." + field_name,
                          " - ", Fore.GREEN + "VALID" + Style.RESET_ALL)
                    valid_data_quantity += 1

                except ValidationError as ex:
                    invalid_data_quantity += 1
                    print(Fore.RED + f"id: {ids[i][0]} {table_name}.{field_name} - NOT VALID" + Style.RESET_ALL)
                    print(Fore.RED + f"Ошибка: {ex.message}" + Style.RESET_ALL)
                    if ex.json_path == "$" and "{" in str(ex.instance):
                        error_log = (f"id: {ids[i][0]} {table_name}.{field_name}\n" + f"Ошибка: {ex.message}\n" +
                                     f"Объект: {ex.instance}\n\n")
                        record_errors_logs(error_log)
                    elif len(ex.json_path.split(".")) > 2:
                        path = ex.json_path.split(".")[1:]
                        object_field = json_data[path[0]]
                        for sub_key in path[1:-1]:
                            object_field = object_field[sub_key]
                        error_log = (f"id: {ids[i][0]} {table_name}.{field_name}\n" + f"Ошибка: '{path[-1]}': {ex.message}\n" +
                                     f"Объект: {object_field}\n\n")
                        record_errors_logs(error_log)
                    else:
                        path = ex.json_path.split(".")[1:]
                        error_log = f"id: {ids[i][0]} {table_name}.{field_name}\n" + f"Ошибка: '{path[-1]}': {ex.message}\n\n"
                        record_errors_logs(error_log)

                except SchemaError as e:
                    error_path = list(e.path)
                    if "is not of type 'object', 'boolean'" in e.message:
                        error_log = f"Схема: {all_schemas_files[index]}\n" + f"Ошибка: Ошибка синтаксиса (не экранирован специальный символ, лишняя скобка или запятая или её не достаёт и тд.) \n\n"
                    else:
                        if len(error_path) >= 3:
                               error_log = f"Схема: {all_schemas_files[index]}\n" + f"Ошибка: {': '.join(error_path[len(error_path) - 3:])}: {e.message}\n\n"
                        elif len(error_path) == 2:
                            error_log = f"Схема: {all_schemas_files[index]}\n" + f"Ошибка: {': '.join(error_path[len(error_path) - 2:])}: {e.message}\n\n"
                        else:
                            error_log = f"Схема: {all_schemas_files[index]}\n" + f"Ошибка: {': '.join(error_path[len(error_path) - 1:])}: {e.message}\n\n"
                    record_errors_logs(error_log)
                    print(Fore.RED + f"Ошибка схемы {all_schemas_files[index]}: {e.message}" + Style.RESET_ALL)
            else:
                valid_data_quantity += 1
    print(Fore.YELLOW + f"Вcего проверено: {all_data_quantity}" + Style.RESET_ALL)
    print(Fore.GREEN + f"Валидных данных: {valid_data_quantity}" + Style.RESET_ALL)
    print(Fore.RED + f"Невалидных данных: {invalid_data_quantity}" + Style.RESET_ALL)


validate_all_json_fields(cursor)
cursor.close()
connection.close()
