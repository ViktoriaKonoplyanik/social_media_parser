"""
Модуль для асинхронного парсинга статистики социальных сетей (ВКонтакте, Telegram)
на основе списка сайтов компаний. Реализует паттерны DTO, Батчинг и Диспетчеризацию.
"""

import asyncio
import logging
import re
import warnings
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from core.config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

warnings.filterwarnings('ignore', message='Unverified HTTPS request')


class SocialNetwork(str, Enum):
    """Перечисление поддерживаемых социальных сетей."""
    VK = 'VK'
    TG = 'Telegram'


VK_API_VERSION = '5.131'
VK_API_URL_GET_BY_ID = "https://api.vk.com/method/groups.getById"
VK_API_URL_SEARCH = "https://api.vk.com/method/groups.search"

TIMEOUT_API = 5
TIMEOUT_WEB = 8
BATCH_SIZE = 10
BATCH_DELAY_SEC = 0.5
MAX_CONCURRENT_CONNECTIONS = 20

SITE_COLUMNS = ['Сайт 1', 'Сайт 2', 'Сайт 3']


@dataclass
class SocialStats:
    """
    Объект переноса данных (DTO) с результатами запроса статистики.

    Attributes:
        is_success: Флаг успешного получения данных.
        count: Количество подписчиков (если доступно).
        message: Статус операции или текст ошибки.
    """
    is_success: bool
    count: Optional[int] = None
    message: str = ""


@dataclass
class CompanyData:
    """
    DTO для хранения извлеченных базовых данных о компании из Excel.

    Attributes:
        name: Название компании (Обособленное Подразделение).
        city: Город дилера.
        sites: Список валидных веб-сайтов компании.
        yandex_url: Ссылка на шоу-рум в Яндекс.Картах.
    """
    name: str
    city: str
    sites: List[str]
    yandex_url: str


@dataclass
class CompanyResult:
    """
    DTO для формирования итоговой строки отчета (Excel).

    Attributes:
        op_name: Название компании (может быть пустым для визуального отступа).
        city: Город.
        site: Список сайтов через запятую.
        account_link: Ссылка на найденную социальную сеть.
        social_network: Название сети (VK/Telegram).
        subscribers_count: Число подписчиков или текстовый статус ошибки.
        debug_log: Журнал операций (как именно была найдена сеть).
    """
    op_name: str
    city: str
    site: str
    account_link: str
    social_network: str
    subscribers_count: Any  # int или str (сообщение об ошибке/скрытии)
    debug_log: str



def normalize_url(url_str: Any) -> str:
    """
    Санитарная обработка и валидация URL-адресов.

    Обрабатывает пустые ячейки (NaN), очищает строки от пробелов и
    принудительно добавляет протокол https:// при его отсутствии.

    Args:
        url_str: Входящее значение из ячейки Pandas (может быть строкой или float/NaN).

    Returns:
        Нормализованная строка URL или пустая строка, если URL невалиден.
    """
    if pd.isna(url_str) or not str(url_str).strip():
        return ""

    clean_str = str(url_str).strip().lower()
    if len(clean_str) < 4:
        return ""

    if not re.match(r'^https?://', clean_str):
        clean_str = 'https://' + clean_str

    parsed = urlparse(clean_str)
    if not parsed.netloc or '.' not in parsed.netloc:
        return ""

    return parsed.geturl()




async def get_vk_subscribers(session: aiohttp.ClientSession, url: str) -> SocialStats:
    """
    Получает количество подписчиков группы ВКонтакте через официальное API.

    Args:
        session: Текущая HTTP-сессия aiohttp.
        url: Ссылка на сообщество ВКонтакте.

    Returns:
        Объект SocialStats с результатами или текстом ошибки сети/приватности.
    """
    domain_match = re.search(r'vk\.com/([a-zA-Z0-9_\.]+)', url)
    if not domain_match:
        return SocialStats(is_success=False, message="Неверная ссылка ВК")

    params = {
        'group_ids': domain_match.group(1),
        'fields': 'members_count',
        'access_token': settings.VK_TOKEN,
        'v': VK_API_VERSION
    }

    try:
        async with session.get(VK_API_URL_GET_BY_ID, params=params, timeout=TIMEOUT_API) as response:
            data = await response.json()
            if 'response' in data and data['response']:
                count = data['response'][0].get('members_count')
                if count is not None:
                    return SocialStats(is_success=True, count=int(count))
                return SocialStats(is_success=False, message="Скрыто")
            return SocialStats(is_success=False, message="Ошибка API ВК")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"Сетевой сбой ВК для {url}: {e}")
        return SocialStats(is_success=False, message=f"Сетевой сбой ВК")


async def get_telegram_subscribers(session: aiohttp.ClientSession, url: str) -> SocialStats:
    """
    Скрейпинг публичной страницы Telegram для получения количества подписчиков.

    Отсекает ID постов из пути URL, маскируется под браузер и конвертирует
    текстовые сокращения (например, "10.4K", "1.2M") в числа.

    Args:
        session: Текущая HTTP-сессия aiohttp.
        url: Ссылка на канал или группу Telegram.

    Returns:
        Объект SocialStats с точным или конвертированным числом подписчиков.
    """
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.strip('/').split('/')
    if not path_parts or path_parts[0] == '':
        return SocialStats(is_success=False, message="Неверный путь TG")

    clean_url = urlunparse((parsed_url.scheme, parsed_url.netloc, f"/{path_parts[0]}", '', '', ''))
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9'
    }

    try:
        async with session.get(clean_url, timeout=TIMEOUT_WEB, ssl=False, headers=headers) as response:
            if response.status != 200:
                return SocialStats(is_success=False, message=f"Ошибка HTTP: {response.status}")

            soup = BeautifulSoup(await response.text(), 'lxml')
            counter_element = soup.find('div', class_='tgme_page_counter') or soup.find('div', class_='tgme_page_extra')

            if counter_element:
                text = counter_element.get_text().lower()

                if 'k' in text or 'м' in text:
                    num_match = re.search(r'\d+[\.,]\d+|\d+', text)
                    if num_match:
                        num_str = num_match.group(0).replace(',', '.')
                        multiplier = 1_000_000 if 'm' in text or 'м' in text else 1_000
                        return SocialStats(is_success=True, count=int(float(num_str) * multiplier))

                count_str = "".join(re.findall(r'\d+', text.replace(' ', '')))
                if count_str:
                    return SocialStats(is_success=True, count=int(count_str))

            return SocialStats(is_success=False, message="Не найдено")

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"Сетевой сбой TG для {url}: {e}")
        return SocialStats(is_success=False, message=f"Сетевой сбой TG")


ParserHandler = Callable[[aiohttp.ClientSession, str], Awaitable[SocialStats]]

NETWORK_HANDLERS: Dict[SocialNetwork, ParserHandler] = {
    SocialNetwork.VK: get_vk_subscribers,
    SocialNetwork.TG: get_telegram_subscribers,
}


async def master_get_subscribers_async(session: aiohttp.ClientSession, url: str, network_name: str) -> SocialStats:
    """
    Фасад (Роутер) для маршрутизации парсинга конкретной соцсети.

    Использует паттерн словаря-диспетчера (NETWORK_HANDLERS) для выбора
    нужной функции обработки, избавляя от конструкций if/elif.

    Args:
        session: HTTP-сессия aiohttp.
        url: Ссылка на профиль соцсети.
        network_name: Текстовое имя соцсети ('VK' или 'Telegram').

    Returns:
        Результат работы изолированного парсера в формате SocialStats.
    """
    if not url:
        return SocialStats(is_success=False, message="Нет ссылки")

    try:
        network_enum = SocialNetwork(network_name)
    except ValueError:
        logger.warning(f"Неизвестная социальная сеть: {network_name} для URL {url}")
        return SocialStats(is_success=False, message=f"Неизвестная сеть: {network_name}")

    handler = NETWORK_HANDLERS.get(network_enum)
    if not handler:
        return SocialStats(is_success=False, message="Нет обработчика")

    return await handler(session, url)


async def parse_socials_from_url_async(session: aiohttp.ClientSession, url: str) -> Dict[str, SocialNetwork]:
    """
    Скрейпинг HTML-страницы дилера для поиска ссылок на социальные сети.

    Игнорирует системные ссылки (шаринг, яндекс.карты) и возвращает
    уникальные совпадения.

    Args:
        session: HTTP-сессия aiohttp.
        url: Ссылка на главную страницу сайта дилера.

    Returns:
        Словарь найденных ссылок вида {URL: Enum социальной сети}.
    """
    target_url = normalize_url(url)
    if not target_url:
        return {}

    try:
        async with session.get(target_url, timeout=TIMEOUT_WEB, ssl=False) as response:
            if response.status != 200:
                return {}

            soup = BeautifulSoup(await response.text(), 'lxml')
            results = {}

            for a in soup.find_all('a', href=True):
                href_lower = a['href'].lower()

                if any(sw in href_lower for sw in ['mapsyandex', 'yandexmaps', 'yandex', 'share', 'intent']):
                    continue

                if 'vk.com' in href_lower:
                    results[a['href']] = SocialNetwork.VK
                elif 't.me' in href_lower or 'telegram.me' in href_lower:
                    results[a['href']] = SocialNetwork.TG

            return results
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return {}


async def search_vk_group_via_api_async(session: aiohttp.ClientSession, company: str, city: str) -> Tuple[
    Dict[str, SocialNetwork], str]:
    """
    Резервный поиск (Фолбэк) группы ВКонтакте через метод API groups.search.

    Используется, если у дилера нет рабочих сайтов. Фильтрует выдачу по
    ключевым словам бренда.

    Args:
        session: HTTP-сессия aiohttp.
        company: Название компании (для поиска).
        city: Название города (для гео-уточнения).

    Returns:
        Кортеж: (Словарь с найденной ссылкой, Текстовый лог операции).
    """
    clean_company = re.sub(r'\(.*?\)', '', company).strip()
    query = f"{clean_company} {city} Alutech Алютех ворота VK"
    params = {'q': query, 'count': 5, 'access_token': settings.VK_TOKEN, 'v': VK_API_VERSION}

    try:
        async with session.get(VK_API_URL_SEARCH, params=params, timeout=TIMEOUT_API) as resp:
            data = await resp.json()
            items = data.get('response', {}).get('items', [])

            for group in items:
                text_to_search = (group.get('name', '') + group.get('description', '')).lower()
                if any(kw in text_to_search for kw in ['алютех', 'alutech', 'ворота']):
                    return {f"https://vk.com/{group['screen_name']}": SocialNetwork.VK}, "Найдено через API"
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return {}, "Ошибка сети при поиске"

    return {}, "Не найдено"


def extract_company_data(row: pd.Series, op_col: str) -> CompanyData:
    """
    Извлекает и структурирует сырые данные одной строки из Pandas.

    Args:
        row: Строка данных из DataFrame (представляет одного дилера).
        op_col: Имя колонки, в которой лежит название компании.

    Returns:
        Структурированный DTO объект CompanyData.
    """
    company_name = str(row.get(op_col, 'Неизвестно')).strip()
    city_name = str(row.get('Город', '')).strip()
    forbidden_values = {'nan', 'нет сайта', 'нет', ''}

    sites = [
        str(row.get(c, '')).strip() for c in SITE_COLUMNS
        if pd.notna(row.get(c)) and str(row.get(c)).strip().lower() not in forbidden_values
    ]

    yandex_val = str(row.get('Ссылка на шоу-рум на Яндекс.Картах', '')).strip()
    if yandex_val.lower() in forbidden_values:
        yandex_val = ""

    return CompanyData(name=company_name, city=city_name, sites=sites, yandex_url=yandex_val)


async def discover_social_links(session: aiohttp.ClientSession, company: CompanyData) -> Tuple[
    Dict[str, SocialNetwork], str]:
    """
    Оркестрирует процесс поиска ссылок на соцсети для одной компании.

    Сначала конкурентно проверяет все сайты компании. В случае неудачи —
    обращается к резервному API поиску ВК.

    Args:
        session: HTTP-сессия aiohttp.
        company: Данные компании (DTO CompanyData).

    Returns:
        Кортеж: (Словарь уникальных соцсетей, Текстовый лог выполнения).
    """
    all_socials: Dict[str, SocialNetwork] = {}

    if company.sites or company.yandex_url:
        tasks = [parse_socials_from_url_async(session, s) for s in company.sites]
        if company.yandex_url:
            tasks.append(parse_socials_from_url_async(session, company.yandex_url))

        for res_dict in await asyncio.gather(*tasks):
            all_socials.update(res_dict)

        log = "Проверены Сайт/Яндекс" if all_socials else "Сайт/Яндекс проверены, соцсети не найдены"
        return all_socials, log

    api_socials, msg = await search_vk_group_via_api_async(session, company.name, company.city)
    return api_socials, f"Поиск через API ВК. Результат: {msg}"


def format_result_rows(company: CompanyData, socials: Dict[str, SocialNetwork], stats: Tuple[SocialStats, ...],
                       log: str) -> List[CompanyResult]:
    """
    Преобразует агрегированные данные по компании в плоский список DTO для отчета.

    Реализует логику визуального отступа: название компании и город записываются
    только в первую строку, а дубликаты скрываются (для красивого экспорта в Excel).

    Args:
        company: Базовые данные о компании.
        socials: Словарь найденных социальных сетей.
        stats: Кортеж с результатами парсинга статистики.
        log: Журнал отладки (событий) для данной компании.

    Returns:
        Список объектов CompanyResult, представляющих строки будущей таблицы.
    """
    sites_str = ", ".join(company.sites)

    if not socials:
        return [CompanyResult(
            op_name=company.name, city=company.city, site=sites_str,
            account_link='', social_network='', subscribers_count='Нет ссылок', debug_log=log
        )]

    rows = []
    for i, ((link, net), stat) in enumerate(zip(socials.items(), stats)):
        op = company.name if i == 0 else ''
        city = company.city if i == 0 else ''
        site = sites_str if i == 0 else ''
        debug = log if i == 0 else ''

        count_display = stat.count if stat.is_success else stat.message

        rows.append(CompanyResult(
            op_name=op, city=city, site=site,
            account_link=link, social_network=net.value,
            subscribers_count=count_display, debug_log=debug
        ))

    return rows


async def process_single_company(session: aiohttp.ClientSession, row: pd.Series, op_col: str) -> List[CompanyResult]:
    """
    Полный пайплайн обработки (Оркестратор локального уровня) для одной компании.

    Args:
        session: HTTP-сессия aiohttp.
        row: Строка данных дилера из Pandas DataFrame.
        op_col: Имя столбца с названием компании.

    Returns:
        Список сформированных строк результата (DTO CompanyResult).
    """
    company = extract_company_data(row, op_col)
    socials, log = await discover_social_links(session, company)

    subs_tasks = [master_get_subscribers_async(session, link, net.value) for link, net in socials.items()]
    stats = await asyncio.gather(*subs_tasks)

    return format_result_rows(company, socials, stats, log)


async def process_dataframe_async(df_actual: pd.DataFrame, op_col: str) -> pd.DataFrame:
    """
    Главный конвейер асинхронной пакетной обработки (Батчинга) всего DataFrame.

    Разбивает таблицу на безопасные пакеты (баты), ограничивает число
    одновременных соединений для защиты от Rate Limit блокировок и склеивает
    результаты в итоговый Pandas DataFrame.

    Args:
        df_actual: Исходная таблица с данными дилеров.
        op_col: Имя столбца с названием компании.

    Returns:
        Новый DataFrame, полностью готовый для экспорта в Excel.
    """
    logger.info("Запуск обработки DataFrame...")
    df_safe = df_actual.copy()
    all_extracted_rows: List[CompanyResult] = []

    # Ограничение соединений для защиты от сетевых банов
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CONNECTIONS)

    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(df_safe), BATCH_SIZE):
            batch = df_safe.iloc[i:i + BATCH_SIZE]
            tasks = [process_single_company(session, row, op_col) for _, row in batch.iterrows()]

            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    logger.exception("Критическая ошибка при обработке компании", exc_info=result)
                    continue
                all_extracted_rows.extend(result)

            await asyncio.sleep(BATCH_DELAY_SEC)

    logger.info("Обработка завершена, формирование итоговой таблицы.")

    # Преобразуем список DTO в DataFrame и переименовываем колонки для финального отчета
    df_analysis = pd.DataFrame([asdict(row) for row in all_extracted_rows])
    df_analysis.rename(columns={
        'op_name': 'ОП',
        'city': 'Город',
        'site': 'Сайт',
        'account_link': 'Ссылка на аккаунт',
        'social_network': 'Соц сеть',
        'subscribers_count': 'Кол-во подписчиков',
        'debug_log': 'Лог (Отладка)'
    }, inplace=True)

    return df_analysis
