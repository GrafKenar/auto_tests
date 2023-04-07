import requests
import json


def forming_json(year, list_parameters):
    parameters = {'region': {'': [list_parameters['region']], '--': []},
                  "program_type": {'': [list_parameters['program_type']], '--': []},
                  "district": {'': [list_parameters['district']], '--': []},
                  "fku": {'': [list_parameters['fku']], '--': []},
                  "road": {'': [list_parameters['road']], '--': []}}
    formed_json = {"date": f"{year}-04-03T10:34:31.954Z",
                   "work_list": parameters["program_type"][list_parameters['program_type_switch']],
                   "region_list": parameters["region"][list_parameters['region_switch']],
                   "district_list": parameters["district"][list_parameters['district_switch']],
                   "fku_list": parameters["fku"][list_parameters['fku_switch']],
                   "object_list": parameters["road"][list_parameters['road_switch']]}
    return formed_json


def http_request_stat_status(year, list_parameters):
    req = requests.post('http://172.16.20.102:8093/stat/status', json=forming_json(year=year, list_parameters=list_parameters))
    response = req.content
    json_deserialized = json.loads(response)
    return json_deserialized


def get_total_reserve_from_api(year, list_parameters):
    json_deserialized = http_request_stat_status(year, list_parameters)
    total_reserve = int(json_deserialized['data']['reserve']['total'])
    return total_reserve


def get_not_started_from_api(year, list_parameters):
    json_deserialized = http_request_stat_status(year, list_parameters)
    count_not_started = int(json_deserialized['data']['not_started']['count'])
    sum_not_started = int(json_deserialized['data']['not_started']['value'])
    return count_not_started, sum_not_started
