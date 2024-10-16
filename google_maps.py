import asyncio
import logging
import os
import re
import googlemaps
from dotenv import load_dotenv
from aiogram.client.session import aiohttp

logger = logging.getLogger(__name__)
load_dotenv()
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")

# Initialize Google Maps client
gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)

# Ключ API и ID поисковой системы для Google Custom Search
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
GOOGLE_CX = os.environ.get("GOOGLE_CX")


async def google_search_and_extract(query):
    all_results = []
    search_result = await fetch_places(query)

    while True:
        info = await process_search_results(search_result)
        all_results.extend(info)

        if 'next_page_token' not in search_result:
            break

        await asyncio.sleep(2)  # Delay to respect API limits
        search_result = await fetch_places(query, search_result['next_page_token'])

    return all_results


async def fetch_places(query, page_token=None):
    try:
        if page_token:
            return gmaps.places(query=query, page_token=page_token, type='establishment', language='en')
        else:
            return gmaps.places(query=query, type='establishment', language='en')
    except Exception as e:
        logger.error(f"Error during fetching places: {str(e)}")
        return {}


async def process_search_results(search_result):
    info = []
    if search_result['status'] == 'OK':
        async with aiohttp.ClientSession() as session:
            tasks = []
            for place in search_result['results']:
                place_id = place['place_id']
                place_details = gmaps.place(place_id=place_id,
                                            fields=['name', 'website', 'formatted_phone_number', 'formatted_address',
                                                    'user_ratings_total'])
                result = place_details['result']
                company_name = result.get('name')
                website = result.get('website', 'No website found')
                phone = result.get('formatted_phone_number', 'No phone found')
                address = result.get('formatted_address', 'No address found')
                reviews_count = result.get('user_ratings_total', 'N/A')

                if website != 'No website found':
                    task = asyncio.create_task(fetch_and_parse_website(session, website))
                    tasks.append((company_name, website, phone, address, reviews_count, task))

            await asyncio.gather(*[t[5] for t in tasks])

            for company_name, website, phone, address, reviews_count, task in tasks:
                emails = await task
                if emails:  # Add sites where emails were found
                    info.append((company_name, website, emails, phone, address, reviews_count))

    return info


async def fetch_and_parse_website(session, url):
    try:
        async with session.get(url) as response:
            html_content = await response.text()
            emails = parse_html(html_content)
            return emails
    except Exception as e:
        logger.error(f"Error fetching or parsing {url}: {str(e)}")
        return []


def parse_html(html_content):
    emails = set(re.findall(r"\b[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]{2,}\b", html_content))
    return filter_emails(emails)


def filter_emails(emails):
    ignore_patterns = [
        r'sentry\..+',
        r'wixpress\.com',
        r'polyfill\.io',
        r'lodash\.com',
        r'core-js-bundle\.com',
        r'react-dom\.com',
        r'react\.com',
        r'npm\.js',
        r'@[a-zA-Z0-9]*[0-9]{5,}@',
        r'\b[a-zA-Z]+@[0-9]+\.[0-9]+\.[0-9]+\b',
        r'@\w*\.png',
        r'@\w*\.jpg',
        r'@\w*\.jpeg',
        r'@\w*\.gif',
        r'\w+-v\d+@3x-\d+x\d+\.png',
        r'\w+-v\d+@3x-\d+x\d+\.png.webp',
        r'[a-zA-Z0-9_\-]+@[0-9]+x[0-9]+\.png',
        r'[a-zA-Z0-9_\-]+@[0-9]+x[0-9]+\.jpeg',
        r'[a-zA-Z0-9_\-]+@[0-9]+x[0-9]+\.png.webp',
        r'[a-zA-Z0-9_\-]+@[\d]+x[\d]+\.png',
        r'[a-zA-Z0-9_\-]+@\d+x\d+\.(png|jpg|jpeg|gif)',
        r'[a-zA-Z0-9_\-]+-v\d+_?\d*@[0-9]+x[0-9]+\.png',
        r'[a-zA-Z0-9_\-]+-v\d+_?\d*@[0-9]+x[0-9]+\.png.webp',
        r'IASC',
        r'@\w*\.png.webp',
        r'Mesa-de-trabajo'
    ]
    return [email for email in emails if not any(re.search(pattern, email) for pattern in ignore_patterns)]
