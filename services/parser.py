import asyncio
import re
import warnings
from typing import List, Tuple, Dict, Any

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from core.config import settings

# Отключаем предупреждения о непроверенных HTTPS-запросах
warnings.filterwarnings('ignore', message='Unverified HTTPS request')


async def master_get_subscribers_async(session: aiohttp.ClientSession, url: str, network: str) -> str:
    """
    Получает количество подписчиков для указанной социальной сети.

    Args:
        session (aiohttp.ClientSession): Асинхронная сессия для HTTP-запросов.
        url (str): Ссылка на профиль в социальной сети.
        network (str): Название социальной сети ('VK' или 'Telegram').

    Returns:
        str: Количество подписчиков в виде строки, 'Скрыто', 'Нет данных' или 'Ошибка' в случае сбоя.
    """
    if not url or url == "Не найдено":
        return ""

    try:
        if network.upper() == 'VK':
            domain_match = re.search(r'vk\.com/([a-zA-Z0-9_\.]+)', url)
            if not domain_match:
                return "Неверная ссылка"

            api_url = "https://api.vk.com/method/groups.getById"
            params = {
                'group_ids': domain_match.group(1),
                'fields': 'members_count',
                'access_token': settings.VK_TOKEN,
                'v': '5.131'
            }

            async with session.get(api_url, params=params, timeout=5) as response:
                data = await response.json()
                if 'response' in data and data['response']:
                    return str(data['response'][0].get('members_count', 'Скрыто'))
                return "Ошибка"

        elif network == 'Telegram':
            clean_url = url.split('?')[0] + '?embed=1'
            async with session.get(clean_url, timeout=8, ssl=False) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    extra = soup.find('div', class_='tgme_page_extra')
                    if extra:
                        return "".join(re.findall(r'\d+', extra.text.replace(' ', '')))
            return "Нет данных"

    except Exception:
        return "Ошибка"

    return ""

async def parse_socials_from_url_async(session: aiohttp.ClientSession, url: str) -> Tuple[List[str], List[str]]:
    """
    Парсит веб-страницу и извлекает ссылки на социальные сети (VK, Telegram).

    Args:
        session (aiohttp.ClientSession): Асинхронная сессия для HTTP-запросов.
        url (str): URL сайта для парсинга.

    Returns:
        Tuple[List[str], List[str]]: Кортеж из двух списков (найденные ссылки и соответствующие им соцсети).
    """
    if pd.isna(url) or not str(url).strip() or len(str(url).strip()) < 5:
        return [], []

    url_str = str(url)
    target_url = f"https://{url_str}" if not url_str.startswith('http') else url_str

    try:
        async with session.get(target_url, timeout=8, ssl=False) as response:
            if response.status != 200:
                return [], []

            soup = BeautifulSoup(await response.text(), 'html.parser')
            links, nets = [], []

            for a in soup.find_all('a', href=True):
                href_lower = a['href'].lower()

                # Пропускаем ссылки, связанные с картами и шерингом
                stop_words = ['mapsyandex', 'yandexmaps', 'yandex', 'share', 'intent']
                if any(stop_word in href_lower for stop_word in stop_words):
                    continue

                if 'vk.com' in href_lower:
                    links.append(a['href'])
                    nets.append('VK')
                elif 't.me' in href_lower or 'telegram.me' in href_lower:
                    links.append(a['href'])
                    nets.append('Telegram')

            return links, nets

    except Exception:
        return [], []


async def search_vk_group_via_api_async(session: aiohttp.ClientSession, company: str, city: str) -> Tuple[
    List[str], List[str], str]:
    """
    Выполняет поиск группы дилера в VK через API, если прямые ссылки не найдены.

    Args:
        session (aiohttp.ClientSession): Асинхронная сессия.
        company (str): Название компании.
        city (str): Город присутствия.

    Returns:
        Tuple[List[str], List[str], str]: Списки найденных ссылок, сетей и статус поиска (лог).
    """
    # Удаляем содержимое в скобках из названия компании (например, организационно-правовую форму)
    regex_pattern = r'\(.*?\)'
    clean_company = re.sub(regex_pattern, '', company).strip()

    query = f"{clean_company} {city} Alutech Алютех ворота VK"
    params = {
        'q': query,
        'count': 5,
        'access_token': settings.VK_TOKEN,
        'v': '5.131'
    }

    try:
        async with session.get("https://api.vk.com/method/groups.search", params=params, timeout=5) as resp:
            data = await resp.json()
            items = data.get('response', {}).get('items', [])

            for group in items:
                text_to_search = (group.get('name', '') + group.get('description', '')).lower()
                keywords = ['алютех', 'alutech', 'ворота']

                if any(keyword in text_to_search for keyword in keywords):
                    return [f"https://vk.com/{group['screen_name']}"], ["VK"], "Найдено через API"

    except Exception:
        pass

    return [], [], "Не найдено"


async def process_single_company(session: aiohttp.ClientSession, row: pd.Series, op_col: str) -> List[Dict[str, Any]]:
    """
    Обрабатывает данные одной компании: собирает сайты, ищет соцсети и парсит подписчиков.

    Args:
        session (aiohttp.ClientSession): Асинхронная сессия.
        row (pd.Series): Строка DataFrame с данными о компании.
        op_col (str): Название колонки с именем компании (Обособленного подразделения).

    Returns:
        List[Dict[str, Any]]: Список словарей с результатами анализа для формирования итогового DataFrame.
    """
    company_name = str(row.get(op_col, 'Неизвестно')).strip()
    city_name = str(row.get('Город', '')).strip()

    forbidden_values = ['nan', 'нет сайта', 'нет', '']

    # Извлекаем сайты компании
    site_cols = ['Сайт 1', 'Сайт 2', 'Сайт 3']
    sites = [
        str(row.get(c, '')).strip() for c in site_cols
        if pd.notna(row.get(c)) and str(row.get(c)).strip().lower() not in forbidden_values
    ]

    # Извлекаем ссылку на Яндекс.Карты
    yandex_val = str(row.get('Ссылка на шоу-рум на Яндекс.Картах', '')).strip()
    if yandex_val.lower() in forbidden_values:
        yandex_val = ""

    sites_str = ", ".join(sites)
    all_links, all_nets, log = [], [], ""

    # Запускаем парсинг указанных сайтов и Яндекс.Карт
    if sites or yandex_val:
        tasks = [parse_socials_from_url_async(session, s) for s in sites]
        if yandex_val:
            tasks.append(parse_socials_from_url_async(session, yandex_val))

        results = await asyncio.gather(*tasks)
        for res_links, res_nets in results:
            all_links.extend(res_links)
            all_nets.extend(res_nets)

        log = "Проверены Сайт/Яндекс" if all_links else "Сайт/Яндекс проверены, но соцсети не найдены"
    else:
        # Fallback: ищем через API VK, если сайтов нет
        all_links, all_nets, msg = await search_vk_group_via_api_async(session, company_name, city_name)
        log = f"В таблице нет данных. Запущен поиск через API ВК. Результат: {msg}"

    # Очистка дубликатов с сохранением порядка
    unique_links, unique_nets = [], []
    for link, net in zip(all_links, all_nets):
        if link not in unique_links:
            unique_links.append(link)
            unique_nets.append(net)

    # Получаем количество подписчиков
    subs_tasks = [master_get_subscribers_async(session, link, net) for link, net in zip(unique_links, unique_nets)]
    subs = await asyncio.gather(*subs_tasks)

    rows = []
    base_info = {
        'ОП': company_name,
        'Город': city_name,
        'Сайт': sites_str,
        'Лог (Отладка)': log
    }

    if not unique_links:
        rows.append(
            {**base_info, 'Ссылка на аккаунт': 'Не найдено', 'Соц сеть': 'Не найдено', 'Кол-во подписчиков': ''})
    else:
        for i, (link, net, sub_count) in enumerate(zip(unique_links, unique_nets, subs)):
            # Заполняем базовую информацию только для первой строки группы (для визуальной чистоты отчета)
            row_info = base_info if i == 0 else {'ОП': '', 'Город': '', 'Сайт': '', 'Лог (Отладка)': ''}
            rows.append({
                **row_info,
                'Ссылка на аккаунт': link,
                'Соц сеть': net,
                'Кол-во подписчиков': sub_count
            })

    return rows


async def process_dataframe_async(df_actual: pd.DataFrame, op_col: str) -> pd.DataFrame:
    """
    Асинхронно обрабатывает батчами весь DataFrame с данными дилеров.

    Args:
        df_actual (pd.DataFrame): Исходный DataFrame с данными.
        op_col (str): Название целевой колонки с именем компании.

    Returns:
        pd.DataFrame: Итоговый DataFrame с результатами парсинга и аналитики.
    """
    columns = ['ОП', 'Город', 'Сайт', 'Ссылка на аккаунт', 'Соц сеть', 'Кол-во подписчиков', 'Лог (Отладка)']
    df_analysis = pd.DataFrame(columns=columns)

    async with aiohttp.ClientSession() as session:
        # Обрабатываем батчами по 10 записей во избежание блокировок и перегрузки соединений
        for i in range(0, len(df_actual), 10):
            batch = df_actual.iloc[i:i + 10]
            tasks = [process_single_company(session, row, op_col) for _, row in batch.iterrows()]

            batch_results = await asyncio.gather(*tasks)

            # Разворачиваем списки словарей и добавляем в итоговый DataFrame
            flat_results = [item for sublist in batch_results for item in sublist]
            if flat_results:
                df_analysis = pd.concat([df_analysis, pd.DataFrame(flat_results)], ignore_index=True)

            await asyncio.sleep(0.5)  # Небольшая пауза между батчами

    return df_analysis
