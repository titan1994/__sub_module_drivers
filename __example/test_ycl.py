import asyncio
from json import dump as jsd
from data_base.metadata.metadata_processing import process_metadata
from data_base.async_click_house import ycl

async def get_meta():
    in_data = [
        {
            "ip": "192.168.46.229",
            "port": 9000,
            "type_db": "ycl",
            "data_bases": [
                {
                    "user": "default",
                    "pswd": "",
                    "name": "mercury",
                    "tables": ""
                }
            ],
            "name": "clickhouse"
        },
        # {
        #     "ip": "localhost",
        #     "port": 5432,
        #     "type_db": "psql",
        #     "data_bases": [
        #         {
        #             "user": "__test_app_core",
        #             "pswd": "__test_app_core",
        #             "name": "__test_app_core",
        #             "tables": ""
        #         }
        #     ],
        #     "name": "postgres"
        # }
    ]

    report = await process_metadata(json_in=in_data)

    with open('z_get_meta.json', 'w', encoding='utf-8') as fb:
        jsd(report, fb, ensure_ascii=False, indent=4)


async def test_delete():

    conn = {
        'host': 'localhost',
        'port': 9000,
        'user': 'default',
        'password': '',
        'database': 'default',
    }

    res = await ycl.delete_data_from_table(
        conn=conn,
        table='__cl_smpb_showcase_data_farmerpassport_meansofpassport',
        filter_data=[
            {
                'name': 'organization',
                'value': '070706797590',
            },
            {
                'name': 'source_form',
                'value': 'Кфх-Прд',
            }
        ]
    )

    print(res)


if __name__ == '__main__':
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(test_delete())
    ioloop.close()
