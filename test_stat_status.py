from Reqs import *
from conftest import*
import pytest


class TestStatStatus:

    @pytest.mark.stat_status_first
    @pytest.mark.parametrize('year', years_list)
    @pytest.mark.parametrize('list_parameters', params_list)
    def test_total_reserve(self, db_connection, list_parameters, year):
        total_reserve_req = get_total_reserve_from_api(year=year, list_parameters=list_parameters)
        total_reserve_db = execute_sql_total_reserve(year=year, cursor=db_connection, list_parameters=list_parameters)
        assert total_reserve_req == total_reserve_db, \
            f"total reserve from db is {total_reserve_db} while total reserve from api is {total_reserve_req}"

    @pytest.mark.stat_status_first
    @pytest.mark.parametrize('year', years_list)
    @pytest.mark.parametrize('list_parameters', params_list)
    def test_not_started(self, db_connection, list_parameters, year):
        not_started_req = get_not_started_from_api(year=year, list_parameters=list_parameters)
        not_started_db = execute_sql_not_started(year=year, cursor=db_connection, list_parameters=list_parameters)
        assert not_started_req[0] == not_started_db[0] and not_started_req[1] == not_started_db[1], \
            f"count/value from db is {not_started_db} while count/value from api is {not_started_req}"
