"""
Перенесен из очень старых проектов - не действителен
"""

# import asyncio
# import requests
# from json import dump as jsd, load as jsl
#
# DEFAULT_JSON_OUT_PATH = '../data_base/metadata/test_parsing/test_json_out.json'
# DEFAULT_JSON_IN_PATH = '../data_base/metadata/test_parsing/test_json.json'
#
#
# async def test_main():
#     with open(DEFAULT_JSON_IN_PATH, 'r', encoding='utf-8') as fobj:
#         json_array = jsl(fobj)
#
#         res = requests.post('http://localhost:5000/get_metadata', json=json_array)
#         if res.ok:
#             result_json = res.json()
#             print(result_json)
#
#             with open(DEFAULT_JSON_OUT_PATH, 'w') as fobj:
#                 jsd(result_json, fobj, ensure_ascii=False, indent=4)
#
#
# if __name__ == '__main__':
#     ioloop = asyncio.get_event_loop()
#     ioloop.run_until_complete(test_main())
#     ioloop.close()
