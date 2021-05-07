"""
Асинхронные запросы по http/https
"""

import httpx
from json import dumps as jsd


async def send_get(url, params=None):
    """
    Асинхронный гет-запрос
    """

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, params=params)
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting {exc.request.url!r}.")
            return None
        return response


async def send_delete(url, params=None):
    """
    Асинхронный delete-запрос
    """

    async with httpx.AsyncClient() as client:
        try:
            response = await client.delete(url, params=params)
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting {exc.request.url!r}.")
            return None
        return response


async def send_post(url, data_body):
    """
    Асинхронный пост-запрос

    :param url: ссылка
    :param data_body: - серриализуемый для джсона объект (словарь или список примитивов)
    """

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=data_body)
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting {exc.request.url!r}.")
            return None
        return response
