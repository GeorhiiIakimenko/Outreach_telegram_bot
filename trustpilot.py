import asyncio
import csv
import json
import logging
import random
import re
from urllib.parse import urljoin
import aiofiles
import aiohttp
import openai
from bs4 import BeautifulSoup
from fuzzywuzzy import process


logger = logging.getLogger(__name__)

# TrustPilot configurations
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Referer': 'https://www.google.com/',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Connection': 'keep-alive'
}

base_url = "https://www.trustpilot.com"


async def gpt_parse_query(prompt):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a helpful assistant that extracts category, country, city, rating, min reviews and max reviews from a query string. "
                        "If a city is provided, determine the country that city belongs to and return the country's abbreviation (ISO 3166-1 alpha-2 code). "
                        "Return the output as a JSON object with keys: 'category', 'country', 'city', 'rating', 'min_reviews' and 'max_reviews'."
                    )
                },
                {
                    "role": "user",
                    "content": f"Query: {prompt}"
                }
            ],
        )

        content = response.choices[0]['message']['content'].strip()
        return content

    except Exception as e:
        logger.error(f"GPT parsing error: {str(e)}")
        return None


# Function to construct the URL with parameters
async def build_trustpilot_url(category_link, country=None, city=None, rating=None):
    url = base_url + category_link
    params = []

    if country:
        params.append(f"country={country}")
    if city:
        params.append(f"location={city}")
    if rating:
        params.append(f"trustscore={rating}")

    if params:
        url += "?" + "&".join(params)

    logger.info(f"Constructed URL: {url}")
    return url


# Function for parsing categories and writing to CSV
async def get_category_link(category_name):
    await parse_and_save_categories()

    with open('categories.csv', mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        categories = list(reader)

    logger.info(f"Category name: {category_name}")
    # Extract keywords from the query
    key_words = set(str(category_name).lower().split())

    best_match = None
    best_score = 0

    for row in categories:
        category = row['Category'].lower()
        # Check for partial matches
        if any(word in category for word in key_words):
            score = len([word for word in key_words if word in category])
            if score > best_score:
                best_score = score
                best_match = row

    if not best_match:
        # If no partial match, use fuzzy matching
        category_names = [row['Category'] for row in categories]
        best_match_name = process.extractOne(category_name, category_names)
        if best_match_name and best_match_name[1] > 60:  # 60% similarity threshold
            best_match = next(row for row in categories if row['Category'] == best_match_name[0])

    if best_match:
        logging.info(f"Found category link for: {category_name} -> {best_match['Category']}")
        return best_match['Link']

    logging.warning(f"Category not found: {category_name}")
    return None


async def parse_and_save_categories():
    response = await aiohttp.ClientSession().get(base_url + "/categories", headers=headers)
    if response.status != 200:
        logging.error(f"Failed to retrieve page: {base_url}/categories")
        return

    soup = BeautifulSoup(await response.text(), 'html.parser')
    categories = soup.find_all('a', class_='link_notUnderlined__szqki')

    category_data = [(category.get_text(), category['href']) for category in categories]

    async with aiofiles.open('categories.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        await writer.writerow(['Category', 'Link'])
        for name, link in category_data:
            await writer.writerow([name, link])

    logging.info(f"Saved all categories")


async def random_delay():
    await asyncio.sleep(random.uniform(1, 5))


async def parse_companies_and_contacts(session, category_link, min_reviews=None, max_reviews=None):
    company_data = []
    seen_companies = set()

    if min_reviews is None:
        min_reviews = 0
    if max_reviews is None:
        max_reviews = 999999

    page_num = 1
    while True:
        paged_url = urljoin(base_url, category_link) + f"&page={page_num}"
        async with session.get(paged_url, headers=headers) as response:
            await random_delay()
            if response.status != 200:
                logger.error(f"Failed to retrieve category page: {paged_url}")
                break  # Exit the loop on error

            logger.info(f"Successfully retrieved category page: {paged_url}")
            soup = BeautifulSoup(await response.text(), 'html.parser')

            # Check for the results count
            results_count_element = soup.find('p', class_='typography_body-m__xgxZ_')
            if results_count_element:
                results_count = int(re.search(r'\d+', results_count_element.get_text()).group())
                if results_count == 0:
                    logger.info("No more results to process.")
                    break  # Exit the loop if there are no results

            companies = soup.find_all('a', attrs={'name': 'business-unit-card'})
            logger.info(f"Found {len(companies)} companies on page {page_num}")

            if not companies:  # If no companies found, exit the loop
                logger.info("No more companies found on this page.")
                break

            for company in companies:
                company_name = re.sub(r'\.com|\.ai', '',
                                      company.find('p', class_='typography_heading-xs__jSwUz').get_text()
                                      .replace('.com', '').strip()).strip().capitalize()
                company_link = company['href']

                if company_name not in seen_companies:
                    logger.info(f"Parsing company: {company_name}")
                    company_details = await parse_company_details(session, company_link)

                    if company_details:
                        rating, email, phone_number, location, verification_status, website, reviews = company_details

                        # Convert reviews to integer and filter based on the provided range
                        try:
                            reviews_count = int(reviews)
                        except ValueError:
                            logger.warning(f"Invalid review count for {company_name}: {reviews}")
                            reviews_count = 0

                        logger.info(f"{min_reviews} <= {reviews_count} <= {max_reviews}")
                        if min_reviews <= reviews_count <= max_reviews:
                            company_data.append((company_name, rating, email, phone_number, location,
                                                 verification_status, website, reviews))
                            seen_companies.add(company_name)
                            logger.info(f"Added company: {company_name} with {reviews} reviews (in range)")
                        else:
                            logger.info(f"Skipped company: {company_name} with {reviews} reviews (out of range)")

            page_num += 1  # Increment the page number for the next iteration

    logger.info(f"Total unique companies parsed: {len(company_data)}")
    return company_data


async def parse_company_details(session, company_link):
    async with session.get(base_url + company_link, headers=headers) as response:
        await random_delay()
        if response.status != 200:
            logger.error(f"Failed to retrieve company page: {base_url + company_link}")
            return None

        logger.info(f"Successfully retrieved company page: {base_url + company_link}")
        soup = BeautifulSoup(await response.text(), 'html.parser')

        # Парсинг email
        email_tag = soup.find('a', href=lambda href: href and "mailto:" in href)
        email = email_tag['href'].replace("mailto:", "") if email_tag else None

        # Парсинг рейтинга
        rating_tag = soup.find('p', class_='typography_body-l__KUYFJ typography_appearance-subtle__8_H2l', attrs={'data-rating-typography': 'true'})
        rating = rating_tag.get_text().strip() if rating_tag else None

        # Парсинг телефона
        phone_tag = soup.find('a', href=lambda href: href and "tel:" in href)
        phone_number = clean_phone_number(phone_tag.get_text().strip()) if phone_tag else None

        # Парсинг локации
        location_tag = soup.find('ul', class_='styles_contactInfoAddressList__RxiJI')
        location = ", ".join([loc.get_text().replace(',', '') for loc in location_tag.find_all('li')]) if location_tag else None

        # Статус верификации
        verification_tag = soup.find('button', class_='styles_verificationLabel__kukuk')
        verification_status = "True" if verification_tag else "False"

        # Парсинг веб-сайта
        website_tag = soup.find('a', class_='link_internal__7XN06 link_wrapper__5ZJEx', href=True)
        website = website_tag['href'] if website_tag else None

        # Парсинг количества отзывов
        reviews_tag = soup.find('span', class_='typography_body-l__KUYFJ typography_appearance-subtle__8_H2l styles_text__W4hWi')
        reviews = "0"

        if reviews_tag:
            reviews_text = reviews_tag.get_text()
            reviews_match = re.search(r'[\d,]+', reviews_text)
            if reviews_match:
                reviews = reviews_match.group().replace(',', '')
            else:
                logger.warning(f"Unable to extract review count from text: {reviews_text}")

        logger.info(f"Parsed details for company - Rating: {rating}, Email: {email}, Phone: {phone_number}, "
                    f"Location: {location}, Verification: {verification_status}, Website: {website}, Reviews: {reviews}")

        return rating, email, phone_number, location, verification_status, website, reviews


def clean_phone_number(phone_number):
    return re.sub(r'\D', '', phone_number)


async def trustpilot_search(query):
    logging.info(f"Processing query for trustpilot: {query}")
    gpt_response = await gpt_parse_query(query)

    if not gpt_response:
        logger.warning("Failed to extract details from query")
        return []

    try:
        cleaned_response = gpt_response.strip().strip('```json').strip('```')

        logger.info(f"Cleaned GPT response: {cleaned_response}")
        parsed_query = json.loads(cleaned_response)

        category = parsed_query.get("category")
        country = parsed_query.get("country")
        city = parsed_query.get("city")
        rating = parsed_query.get("rating", 0)
        min_reviews = parsed_query.get("min_reviews", 0)
        max_reviews = parsed_query.get("max_reviews", 999999)

        category_link = await get_category_link(category)

        if category_link:
            trustpilot_url = await build_trustpilot_url(category_link, country, city, rating)

            # Ensure proper session management
            async with aiohttp.ClientSession() as session:
                return await parse_companies_and_contacts(session, trustpilot_url, min_reviews, max_reviews)

        logger.warning(f"Failed to find a matching category for: {category}")
        return []

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse GPT response: {gpt_response} with error {e}")
        return []  # Handle error by returning empty or as needed
