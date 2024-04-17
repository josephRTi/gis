import json
from pprint import pprint


def test_get_tables(client):
    response = client.get("/gis_ksodd/gis/tables")
    assert 'count' in response.get_json()
    assert 'success' == response.get_json().get('message')
    assert response.status_code == 200


def test_get_table_id(client, new_tables):
    response_1 = client.get(f"/gis_ksodd/gis/{new_tables[0]}")
    response_2 = client.get(f"/gis_ksodd/gis/{new_tables[1]}")
    response_3 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}")

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert response_3.status_code == 200

    assert response_1.get_json().get('pages') == 18
    assert response_2.get_json().get('pages') == 2
    assert response_3.get_json().get('pages') == 15


def test_get_table_id_limit(client, new_tables):
    response_1 = client.get(f"/gis_ksodd/gis/{new_tables[0]}?limit=20")
    response_2 = client.get(f"/gis_ksodd/gis/{new_tables[1]}?limit=10")
    response_3 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}?limit=2")

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert response_3.status_code == 200

    assert response_1.get_json().get('pages') == 45
    assert response_2.get_json().get('pages') == 6
    assert response_3.get_json().get('pages') == 369


def test_get_table_id_pagination(client, new_tables):
    response_1 = client.get(f"/gis_ksodd/gis/{new_tables[0]}?limit=2&page=1")
    response_2 = client.get(f"/gis_ksodd/gis/{new_tables[1]}?limit=3&page=1")
    response_3 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}?limit=4&page=1")

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert response_3.status_code == 200

    assert response_1.get_json().get('pages') == 446
    assert response_2.get_json().get('pages') == 19
    assert response_3.get_json().get('pages') == 185

    assert len(response_1.get_json().get('data')) == 2
    assert len(response_2.get_json().get('data')) == 3
    assert len(response_3.get_json().get('data')) == 4


def test_get_table_id_filter_for_field(client, new_tables):
    response_1 = client.get(f"/gis_ksodd/gis/{new_tables[0]}?gis_ksodd_id=1")
    response_2 = client.get(f"/gis_ksodd/gis/{new_tables[1]}?F2=ул.Газетная - ул.Первомайская")
    response_3 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}?gis_ksodd_id=1050000000")

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert response_3.status_code == 200

    assert response_1.get_json().get('data')[0]['abbrev'] == 'SSE'
    assert response_2.get_json().get('data')[0]['geometry'] == {'coordinates': [59.961912, 57.912586], 'type': 'Point'}
    assert response_3.get_json().get('data') is None


def test_get_table_id_sortby(client, new_tables):
    response_1 = client.get(f"/gis_ksodd/gis/{new_tables[0]}?sortby=abbrev")
    response_2 = client.get(f"/gis_ksodd/gis/{new_tables[1]}?sortby=-F2")
    response_3 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}?sortby=osm_id")

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert response_3.status_code == 200

    assert response_1.get_json().get('data')[0]['abbrev'] == 'ABJ'
    assert response_2.get_json().get('data')[0]['F2'] == 'Фестивальная - ЦОТ'
    assert response_3.get_json().get('data')[0]['osm_id'] == '10250241'


def test_get_table_id_mask(client, new_tables):
    response_1 = client.get(f"/gis_ksodd/gis/{new_tables[0]}?mask=abbrev=A%")
    response_2 = client.get(f"/gis_ksodd/gis/{new_tables[1]}?mask=F2=%_ая%")
    response_3 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}?mask=name=%a")

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert response_3.status_code == 200

    assert len(response_1.get_json().get('data')) == 51
    assert len(response_2.get_json().get('data')) == 28
    assert len(response_3.get_json().get('data')) == 7


def test_get_table_id_filter_more_less(client, new_tables):
    response_1 = client.get(f"/gis_ksodd/gis/{new_tables[0]}?filter=abbrev>=Z")
    response_2 = client.get(f"/gis_ksodd/gis/{new_tables[1]}?filter=F8<=2")
    response_3 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}?filter=gis_ksodd_id>100")

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert response_3.status_code == 200

    pprint(response_2.get_json().get('data'))
    assert len(response_1.get_json().get('data')) == 9
    assert len(response_2.get_json().get('data')) == 48
    assert len(response_3.get_json().get('data')) == 636


def test_get_table_id_ksodd_date_filter(client, new_tables):
    response_1 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}?from=2022.12.22")
    response_2 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}?to=2023.12.22")
    response_3 = client.get(f"/gis_ksodd/ksodd/{new_tables[2]}?from=2022.12.22&to=2023.12.22")

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert response_3.status_code == 200

    assert len(response_1.get_json().get('data')) == 727
    assert len(response_2.get_json().get('data')) == 725
    assert len(response_3.get_json().get('data')) == 715


def test_spatial_filter_table_id(client, new_tables):
    mimetype = 'application/json'
    headers = {
        'Content-Type': mimetype,
        'Accept': mimetype
    }
    data = {
        "geometry": {
            "coordinates":
                [
                    19.9039385,
                    54.6426708
                ],
            "type": "Point"
        }
    }
    response_1 = client.post(f"/gis_ksodd/ksodd/{new_tables[2]}", data=json.dumps(data), headers=headers)

    assert response_1.status_code == 200
    assert response_1.get_json()[0]['gis_ksodd_id'] == 612
    assert response_1.get_json()[1]['gis_ksodd_id'] == 736



