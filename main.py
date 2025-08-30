import json
from json import JSONDecodeError
from jsonschema import validate, ValidationError
from jsonschema._format import FormatChecker
from jsonschema.exceptions import SchemaError, UnknownType, UndefinedTypeCheck
import os
import psycopg2
from psycopg2 import OperationalError
import argparse
import yaml
import colorama
from colorama import Fore, Style
import datetime

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


def execute_query(cursor: "объект типа cursor", query: str) -> list:
    cursor.execute(query)
    data = cursor.fetchall()
    return data


def get_files_from_folder(path: str) -> list:
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    return files


def record_errors_logs(error_log: str, record_date: str, record_time: str) -> None:
    parent_dir = os.path.dirname(MAIN_SCHEMAS_FOLDER_PATH)
    file_path = os.path.join(parent_dir, f"Errors_Logs_{record_date}_{record_time}.txt")
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(error_log)


def get_table_rows(cursor: "объект типа cursor", field_name: str, table_name: str) -> list:
    query_for_rows = f"SELECT {field_name}::text FROM {table_name}"
    row = execute_query(cursor, query_for_rows)
    return row


def get_table_ids(cursor: "объект типа cursor", table_name: str) -> list:
    query_for_ids = f"SELECT id FROM {table_name}"
    ids = execute_query(cursor, query_for_ids)
    return ids


# Функция для получения типов users или datasources
def get_types(cursor: "объект типа cursor", table_name: str) -> list:
    query_for_types = ""
    if table_name == "settings.users":
        query_for_types = (f"SELECT up.name FROM {table_name} "
                           f"JOIN dictionaries.user_paths up ON {table_name}.path = up.id")
    elif table_name == "settings.datasources":
        query_for_types = (f"SELECT up.name FROM {table_name} "
                           f"JOIN dictionaries.user_paths up ON {table_name}.user_path = up.id")
    else:
        print("Типы пока обрабатываются только в таблицах users и datasources")
    all_types = [i[0] for i in execute_query(cursor, query_for_types)]
    return all_types


def add_new_fields_types(table_name: str, data: dict, type: str) -> None:
    if table_name == "settings.users":
        data["user_type"] = type
    elif table_name == "settings.datasources":
        data["datasource_type"] = type
        if "send_conn_info" in data.keys():
            data["queue_mode"] = "send"
        elif "receive_conn_info" in list(data.keys()):
            data["queue_mode"] = "receive"


def validate_all_json_fields(cursor: "объект типа cursor", all_schemas_files: list) -> None:
    invalid_data_quantity = 0
    valid_data_quantity = 0
    all_data_quantity = 0
    current_date = datetime.date.today().isoformat()
    current_time = str(datetime.datetime.now().time()).split(".")[0].replace(":", ".")
    for index, schema_file in enumerate(all_schemas_files, start=0):
        schema_tablename_dict_key = '.'.join(all_schemas_files[index].split(".")[:-1])
        table_name = '.'.join(TABLE_SCHEMA_DICT[schema_tablename_dict_key].split(".")[:-1])
        field_name = TABLE_SCHEMA_DICT[schema_tablename_dict_key].split(".")[-1]
        rows = get_table_rows(cursor, field_name, table_name)
        ids = get_table_ids(cursor, table_name)

        json_schema = open(
            f"{MAIN_SCHEMAS_FOLDER_PATH}\\{all_schemas_files[index]}",
            "r",
            encoding="utf-8"
        )

        try:
            deserialized_json_schema = json.load(json_schema)
        except JSONDecodeError as ex:
            print(Fore.RED + f"Ошибка при десериализации json-схемы: {ex.msg} {ex.doc}" + Style.RESET_ALL)
            deserialized_json_schema = json_schema

        for i, row_tuple in enumerate(rows, start=0):
            all_data_quantity += 1

            if row_tuple[0] is not None:
                json_data = json.loads(row_tuple[0])
                if table_name == "settings.users" or table_name == "settings.datasources":
                    types = get_types(cursor, table_name)
                    add_new_fields_types(table_name, json_data, types[i])

                try:
                    validate(instance=json_data, schema=deserialized_json_schema, format_checker=FormatChecker())
                    print("Id: ", ids[i][0], table_name + "." + field_name,
                          " - ", Fore.GREEN + "VALID" + Style.RESET_ALL)
                    valid_data_quantity += 1

                except ValidationError as ex:
                    invalid_data_quantity += 1
                    print(Fore.RED + f"Id: {ids[i][0]} {table_name}.{field_name} - NOT VALID" + Style.RESET_ALL)
                    print(Fore.RED + f"Ошибка: {ex.message}" + Style.RESET_ALL)
                    if ex.json_path == "$" and "{" in str(ex.instance):
                        error_log = (
                            f"Id: {ids[i][0]} {table_name}.{field_name}\n" + f"Ошибка: {ex.message}\n" +
                            f"Объект: {ex.instance}\n\n"
                        )
                        record_errors_logs(error_log, current_date, current_time)
                    elif len(ex.json_path.split(".")) > 2:
                        path = ex.json_path.split(".")[1:]
                        object_field = json_data[path[0]]
                        for sub_key in path[1:-1]:
                            object_field = object_field[sub_key]
                        error_log = (
                            f"Id: {ids[i][0]} {table_name}.{field_name}\n" + f"Ошибка: '{path[-1]}': {ex.message}\n" +
                            f"Объект: {object_field}\n\n"
                        )
                        record_errors_logs(error_log, current_date, current_time)
                    elif "is not a" in ex.message:
                        path = list(ex.schema_path)
                        error_log = (
                            f"Id: {ids[i][0]} {table_name}.{field_name}\n" +
                            f"Ошибка: '{json_data[path[-2]]}' {' '.join(ex.message.split(" ")[1:])} format\n\n"
                        )
                        record_errors_logs(error_log, current_date, current_time)
                    else:
                        path = ex.json_path.split(".")[1:]
                        error_log = (
                            f"Id: {ids[i][0]} {table_name}.{field_name}\n" +
                            f"Ошибка: '{path[-1]}': {ex.message}\n\n"
                        )
                        record_errors_logs(error_log, current_date, current_time)

                except SchemaError as e:
                    error_path = list(e.path)
                    if "is not of type 'object', 'boolean'" in e.message:
                        error_log = (
                            f"Id: {ids[i][0]}\nОшибка схемы: {all_schemas_files[index]}\n" +
                            f"Ошибка: Ошибка синтаксиса (не экранирован специальный символ,"
                            f" лишняя скобка или запятая или её не достаёт и тд.) \n\n"
                        )
                    else:
                        if len(error_path) >= 3:
                            error_log = (
                                f"Id: {ids[i][0]}\nОшибка схемы: {all_schemas_files[index]}\n" +
                                f"Ошибка: {'-> '.join(error_path[len(error_path) - 3:])}: {e.message}\n\n"
                            )
                        elif len(error_path) == 2:
                            error_log = (
                                f"Id: {ids[i][0]}\nОшибка схемы: {all_schemas_files[index]}\n" +
                                f"Ошибка: {'-> '.join(error_path[len(error_path) - 2:])}: {e.message}\n\n"
                            )
                        else:
                            error_log = (
                                f"Id: {ids[i][0]}\nОшибка схемы: {all_schemas_files[index]}\n" +
                                f"Ошибка: {'-> '.join(error_path[len(error_path) - 1:])}: {e.message}\n\n"
                            )
                    record_errors_logs(error_log, current_date, current_time)
                    print(
                        Fore.RED + f"Id: {ids[i][0]}\nОшибка схемы {all_schemas_files[index]}: "
                                   f"{e.message}" + Style.RESET_ALL
                    )
                except UnknownType as ex:
                    error_args = list(ex.args)
                    invalid_subschema_name = ""
                    for sub_schema in deserialized_json_schema["$defs"]:
                        if str(error_args[-1]) in str(deserialized_json_schema["$defs"][sub_schema]):
                            invalid_subschema_name = sub_schema
                            break
                    print(
                        Fore.RED + f"Id: {ids[i][0]}\nОшибка схемы {all_schemas_files[index]}: "
                                   f"Неопознанный тип" + Style.RESET_ALL
                    )
                    error_log = (
                        f"Id: {ids[i][0]}\nОшибка схемы: {all_schemas_files[index]} -> {invalid_subschema_name}\n" +
                        f"Ошибка: {error_args[2]} Неопознанный тип: '{error_args[0]}'\n\n"
                    )
                    record_errors_logs(error_log, current_date, current_time)
                except UndefinedTypeCheck as ex:
                    error_args = list(ex.args)
                    invalid_subschema_name = ""
                    for sub_schema in deserialized_json_schema["$defs"]:
                        if str(error_args[-1]) in str(deserialized_json_schema["$defs"][sub_schema]):
                            invalid_subschema_name = sub_schema
                            break
                    print(
                        Fore.RED + f"Id: {ids[i][0]}\nОшибка схемы {all_schemas_files[index]}:"
                                   f" Для данного валидатора не зарегистрирован данный тип" + Style.RESET_ALL
                    )
                    error_log = (
                        f"Id: {ids[i][0]}\nОшибка схемы: {all_schemas_files[index]} -> {invalid_subschema_name}\n" +
                        f"Ошибка: {error_args[2]} Для данного валидатора не зарегистрирован данный тип: "
                        f"'{error_args[0]}'\n\n"
                    )
                    record_errors_logs(error_log, current_date, current_time)
            else:
                valid_data_quantity += 1
    print(Fore.YELLOW + f"Вcего проверено: {all_data_quantity}" + Style.RESET_ALL)
    print(Fore.GREEN + f"Валидных данных: {valid_data_quantity}" + Style.RESET_ALL)
    print(Fore.RED + f"Невалидных данных: {invalid_data_quantity}" + Style.RESET_ALL)


if __name__ == "__main__":
    connection = connect_to_db(get_connection_data())
    cursor = get_cursor(connection)
    all_schemas_files = get_files_from_folder(MAIN_SCHEMAS_FOLDER_PATH)

    validate_all_json_fields(cursor, all_schemas_files)
    cursor.close()
    connection.close()
