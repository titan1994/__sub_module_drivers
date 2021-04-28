import asyncio
from json import dump as jsd
from data_base.metadata.metadata_processing import process_metadata


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


if __name__ == '__main__':
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(get_meta())
    ioloop.close()
