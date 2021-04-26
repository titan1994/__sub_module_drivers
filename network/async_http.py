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
        response = await client.get(url, params=params)
        return response


async def send_delete(url, params=None):
    """
    Асинхронный delete-запрос
    """

    async with httpx.AsyncClient() as client:
        response = await client.delete(url, params=params)
        return response


async def send_post(url, data_body):
    """
    Асинхронный пост-запрос

    :param url: ссылка
    :param data_body: - серриализуемый для джсона объект (словарь или список примитивов)
    """

    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=data_body)
        return response
