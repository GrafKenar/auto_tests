import pytest
from Querries import *


@pytest.fixture(scope='class')
def db_connection():
    print("\nestablishing connection to the db")
    conn = psycopg2.connect(host='172.16.21.104', database='risadperf2_migrator', user='postgres', password='postgres')
    cursor = conn.cursor()
    yield cursor
    print("\nclosing connection to the db")
    cursor.close()
    conn.close()


years_list = [2036, 2037, 2038, 2039, 2040, 2041, 2042]

list_parameters0_00000 = {'program_type_switch': '--', 'program_type': '',
                          'region_switch': '--', 'region': '',
                          'district_switch': '--', 'district': '',
                          'fku_switch': '--', 'fku': '',
                          'road_switch': '--', 'road': ''}

list_parameters1_10000 = {'program_type_switch': '', 'program_type': '1aabf23b-0b63-4060-9c0d-056dc5718b93',
                          'region_switch': '--', 'region': '',
                          'district_switch': '--', 'district': '',
                          'fku_switch': '--', 'fku': '',
                          'road_switch': '--', 'road': ''}

list_parameters2_01000 = {'program_type_switch': '--', 'program_type': '',
                          'region_switch': '', 'region': '52ee1be7-7e27-422b-8f01-a876b0d3375a',
                          'district_switch': '--', 'district': '',
                          'fku_switch': '--', 'fku': '',
                          'road_switch': '--', 'road': ''}

params_list = [list_parameters0_00000, list_parameters1_10000, list_parameters2_01000]
