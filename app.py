import asyncio
import csv
import io
import logging
import os
import re
import smtplib
from datetime import datetime
from email.header import Header
from email.mime.text import MIMEText
import aiofiles
import aiohttp
import openai
import whisper
from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher.router import Router
from aiogram.filters import Command
from aiogram.filters.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google_maps import google_search_and_extract
from trustpilot import trustpilot_search


class EmailStates(StatesGroup):
    awaiting_recipient_name = State()
    awaiting_sender_email = State()
    awaiting_phone_number = State()
    awaiting_full_name = State()
    awaiting_job_title = State()
    awaiting_company_name = State()
    awaiting_password = State()
    awaiting_email_theme = State()
    awaiting_draft_review = State()
    awaiting_csv_source = State()
    awaiting_csv_upload = State()


class AnswerStates(StatesGroup):
    answer_text = State()
    answer_draft = State()
    answer_correct = State()
    awaiting_sender_email_answer = State()
    awaiting_password_answer = State()


# Define your states
awaiting_email = True

load_dotenv()

# Set up logging to display information in the console.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Настройки для Google Sheets
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE")
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file'
]
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
sheets_service = build('sheets', 'v4', credentials=credentials)
drive_service = build('drive', 'v3', credentials=credentials)

# Bot token obtained from BotFather in Telegram.
TOKEN = os.environ.get("TELEGRAM_TOKEN")
bot = Bot(token=TOKEN)
router = Router()
router_email = Router()
router_search = Router()
router_answer = Router()
router_linkedin = Router()

# Load Whisper model
model = whisper.load_model("tiny")

# Set your OpenAI API key here
openai.api_key = os.environ.get("OPENAI_API_KEY")


async def handle_voice(message: types.Message):
    file_info = await bot.get_file(message.voice.file_id)
    file_path = await bot.download_file(file_info.file_path)
    with open("voice_message.ogg", "wb") as f:
        f.write(file_path.read())
    result = model.transcribe("voice_message.ogg")
    text = result['text']
    logger.info(f"Transcribed text from voice: {text}")
    await handle_text_query(message, text)


# Define a message handler for the "/start" command.
@router.message(Command("start"))
async def start_message(message: types.Message):
    await message.answer("Hello! Use /search your query by text and after search use /send_email to start sending ")


@router_search.message(Command("search"))
async def handle_text_query(message: types.Message):
    user_input = message.text
    queries = await generate_search_queries(user_input)
    all_results = []

    for query in queries:
        clean_query = re.sub(r'^\d+\.\s*"', '', query).strip('"')
        if clean_query:
            logging.info(f"Processing query for google maps: {clean_query}")
            results = await google_search_and_extract(clean_query)
            for result in results:
                all_results.append(('Google Maps', result))
            logging.info(f"Results found for {clean_query}: {len(results)}")

    trustpilot_info = await trustpilot_search(user_input)
    for info in trustpilot_info:
        all_results.append(('TrustPilot', info))

    if not all_results:
        logging.info("No results found.")
        await message.answer("No results found.")
        return

    logging.info(f"Total results found: {len(all_results)}")

    csv_data = await create_csv(all_results)
    await send_csv_to_telegram(message.chat.id, csv_data)
    logging.info("CSV file sent to Telegram")

    # Запись данных в Google Sheets
    spreadsheet_id = await create_google_sheet(all_results)
    logging.info(f"Data written to Google Sheets. Spreadsheet ID: {spreadsheet_id}")


async def generate_search_queries(user_input):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Generate three diverse search queries for local business information based on the user's input."},
                {"role": "user", "content": user_input}
            ],
            max_tokens=150
        )
        if 'choices' in response and response['choices']:
            full_text = response['choices'][0]['message']['content'].strip()
            queries = full_text.split("\n")  # Splitting by newline to separate the queries
            queries = [query.strip().strip('"') for query in queries if query]  # Clean up each query
            if len(queries) < 2:
                queries += [""] * (2 - len(queries))  # Ensure there are exactly three queries
            logger.info(f"Generated Queries: {queries}")
            return queries
        else:
            logger.warning("No choices returned by GPT-3.")
            return [""] * 2
    except Exception as e:
        logger.error(f"Error generating GPT queries: {str(e)}")
        return [""] * 2



async def create_csv(data):
    logging.info("Creating CSV file")
    output = io.StringIO(newline='')
    writer = csv.writer(output, quoting=csv.QUOTE_ALL)
    writer.writerow(['Company Name', 'Website', 'Emails/Contact Info', 'Phone', 'Location', 'Rating', 'Reviews', 'Verification'])

    for source, item in data:
        try:
            if source == 'TrustPilot' and isinstance(item, tuple):  # TrustPilot results
                name, rating, emails, phone, location, verify, website, reviews = item + ('N/A',) * (8 - len(item))
                writer.writerow([name, website, emails, phone, location, rating, reviews, verify])
            elif source == 'Google Maps' and isinstance(item, tuple):  # Google Maps results
                name, website, emails, phone, address, reviews_count = item + ('N/A',) * (6 - len(item))
                emails_str = ', '.join(emails) if isinstance(emails, list) else str(emails)
                writer.writerow([name, website, emails_str, phone, address, 'N/A', reviews_count, 'N/A'])
            else:
                logging.warning(f"Unexpected data format or source: {source}, {item}")
                row_data = list(item) if isinstance(item, tuple) else [str(item)]
                row_data = row_data + ['N/A'] * (8 - len(row_data))
                writer.writerow(row_data[:8])
        except Exception as e:
            logging.error(f"Error processing item: {source}, {item}. Error: {str(e)}")
            row_data = [str(item)] + ['N/A'] * 7
            writer.writerow(row_data[:8])

    output.seek(0)
    return output


async def send_csv_to_telegram(chat_id, csv_data):
    # Преобразуем StringIO в байты с явной UTF-8 кодировкой
    byte_data = csv_data.getvalue().encode('utf-8')

    form_data = aiohttp.FormData()
    form_data.add_field('document',
                        byte_data,
                        filename='companies_results.csv',
                        content_type='text/csv')

    logging.info("Sending CSV file to Telegram")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f'https://api.telegram.org/bot{TOKEN}/sendDocument?chat_id={chat_id}',
                                    data=form_data) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logging.error(f"Failed to send CSV. Status: {resp.status}, Response: {error_text}")
                else:
                    logging.info("CSV file sent successfully to Telegram")
        except Exception as e:
            logging.error(f"Error sending CSV file to Telegram: {str(e)}")


async def create_google_sheet(data):
    # Create a new table
    spreadsheet = {
        'properties': {
            'title': f'Search Results {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        }
    }
    spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet, fields='spreadsheetId').execute()
    spreadsheet_id = spreadsheet.get('spreadsheetId')
    logging.info(f'New Spreadsheet created. ID: {spreadsheet_id}')

    # Create a new sheet
    sheet_title = f'Results {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
    requests = [{
        "addSheet": {
            "properties": {
                "title": sheet_title
            }
        }
    }]
    body = {'requests': requests}
    response = sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    logging.info(f"Added new sheet: {sheet_title}")

    # Write data
    values = [['Company Name', 'Website/Rating', 'Emails/Contact Info', 'Phone', 'Location', 'Verification']]
    for item in data:
        if isinstance(item, tuple):
            if len(item) >= 3:  # Google Maps results
                name, website, emails = item[:3]
                emails_str = ', '.join(emails) if isinstance(emails, list) else str(emails)
                values.append([name, website, emails_str, 'N/A', 'N/A', 'N/A'])
            elif len(item) == 6:  # TrustPilot results
                values.append(list(item))
            else:
                logging.warning(f"Unexpected data format: {item}")
        else:
            logging.warning(f"Unexpected item type in data: {type(item)}")

    body = {'values': values}
    result = sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f'{sheet_title}!A1',
        valueInputOption='RAW', body=body).execute()
    logging.info(f"{result.get('updatedCells')} cells updated.")

    # Granting access
    permission = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': 'Ivangul999@gmail.com'
    }
    drive_service.permissions().create(fileId=spreadsheet_id, body=permission).execute()
    logging.info('Access granted to Ivangul999@gmail.com')

    return spreadsheet_id


# Start the command to input the sender's email address
@router.message(Command("send_email"))
async def send_email_command(message: types.Message, state: FSMContext):
    await message.answer("Please enter your email address:")
    await state.set_state(EmailStates.awaiting_sender_email)


# Handle the email address input from the user
@router.message(EmailStates.awaiting_sender_email)
async def handle_sender_email(message: types.Message, state: FSMContext):
    sender_email = message.text
    if is_valid_email(sender_email):
        await message.answer("Sender email set. Please enter your phone number:")
        await state.update_data(sender_email=sender_email)
        await state.set_state(EmailStates.awaiting_phone_number)
    else:
        await message.answer("Please enter a valid email address.")


# Utility function to validate an email address format
def is_valid_email(email):
    pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return re.match(pattern, email) is not None


# Handle the phone number input from the user
@router.message(EmailStates.awaiting_phone_number)
async def handle_phone_number(message: types.Message, state: FSMContext):
    phone_number = message.text
    await state.update_data(phone_number=phone_number)
    await message.answer("Phone number set. Please enter your password for SMTP authentication:")
    await state.set_state(EmailStates.awaiting_password)


# Handle the password input for SMTP authentication
@router.message(EmailStates.awaiting_password)
async def handle_password(message: types.Message, state: FSMContext):
    password = message.text
    await state.update_data(password=password)
    await message.answer("Password set. What is the theme or main content for your email?(Write your Name.Surname.Job title.Company Name.")
    await state.set_state(EmailStates.awaiting_email_theme)


# Handle the email theme/content input and generate a draft using OpenAI
@router.message(EmailStates.awaiting_email_theme)
async def handle_email_theme(message: types.Message, state: FSMContext):
    prompt = message.text
    data = await state.get_data()
    sender_email = data['sender_email']
    phone_number = data['phone_number']

    with open('example.html', 'r', encoding='utf-8') as file:
        example = file.read()

    draft = await generate_email_content(prompt, sender_email, phone_number, example)
    logger.info(f"Generated email content: {draft}")
    subject, draft = await generate_email_content(prompt, sender_email, phone_number, example)
    if draft:
        await message.answer(
            f"Subject: {subject}\n\nHere is a draft based on your input:\n{draft}\nDo you approve this draft? Type 'yes' to approve, or provide your corrections.")
        await state.update_data(draft=draft, subject=subject)  # Сохраняем тему
        await state.set_state(EmailStates.awaiting_draft_review)

    else:
        await message.answer("Failed to generate draft, please try entering the theme again.")


async def generate_email_content(prompt, sender_email, phone_number, example):
    try:
        logging.debug("Generating GPT response")
        context = f"Here is an example of email context: {example}"
        # Generate the email content
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a skilled email writer for gmail for less then 600 tokens. Create a professional business email based on the user's provided theme. "
                        "The email should be concise, polite, and aimed at establishing a professional relationship. Use a formal tone "
                        "and structure the email with appropriate greetings to [Recipient's Company] team and body content."
                        "details of the sender. Ensure the email is structured into clear paragraphs, where write name of paragraphs and highlight it."
                        f"Use this file as a base structure {context}, write html code for email. You write to [Recipient's Company] team dont use [Recipient's Name]. Use only placeholders like [Recipient's Company]."
                        "Separate each paragraph with two newlines."
                        "Don't write contact information."
                    )
                },
                {
                    "role": "user",
                    "content": f"Theme: {prompt}. Please include placeholders for the recipient's name and company."
                }
            ],
            max_tokens=600
        )

        content = response.choices[0].message['content'].strip().replace('```html', '').replace('```', '')

        # Extracting a suitable header from the prompt
        header_response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a skilled email writer. Based on the provided theme, generate a suitable and concise email subject line.Example:Exploring Partnership Opportunities with ... "
                        "The subject should be clear, engaging, and relevant to the email content. Keep it short, ideally within 60 characters."
                    )
                },
                {
                    "role": "user",
                    "content": f"Theme: {prompt}"
                }
            ],
            max_tokens=60
        )

        header = header_response.choices[0].message['content'].strip()

        # Split content into paragraphs
        paragraphs = content.split('\n\n')
        formatted_content = ''.join(f'<p>{para}</p>' for para in paragraphs)

        # Construct the HTML email content with a dynamic header
        html_content = f"""
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                }}
                .header {{
                    background-color: #f8f8f8;
                    padding: 10px;
                    text-align: center;
                    border-bottom: 1px solid #ddd;
                }}
                .content {{
                    padding: 20px;
                }}
                .footer {{
                    padding: 10px;
                    text-align: center;
                    border-top: 1px solid #ddd;
                    margin-top: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{header}</h1>
            </div>
            <div class="content">
                {formatted_content}
            </div>
            <div class="footer">
                <p>Phone: {phone_number}</p>
                <p>Email: {sender_email}</p>
            </div>
        </body>
        </html>
        """

        return header, html_content
    except Exception as e:
        print(f"Error generating email content: {str(e)}")
        return None


# Handle the review and approval of the generated email draft
@router.message(EmailStates.awaiting_draft_review)
async def handle_draft_review(message: types.Message, state: FSMContext):
    if message.text:
        response = message.text.lower()
        if response == 'yes':
            await message.answer("Please type 'upload' to upload your CSV or 'default' to use the default CSV.")
            await state.set_state(EmailStates.awaiting_csv_source)
        else:
            await state.update_data(draft=response)
            await message.answer("Draft updated. Type 'yes' to send or provide further corrections.")
    else:
        await message.answer("Please send a text response.")


# Handle the selection between uploading a new CSV file or using a default CSV file
@router.message(EmailStates.awaiting_csv_source)
async def choose_csv_source(message: types.Message, state: FSMContext):
    if message.text:
        user_input = message.text.lower()
        if user_input == 'upload':
            await message.answer("Please upload your CSV file.")
            await state.set_state(EmailStates.awaiting_csv_upload)
        elif user_input == 'default':
            data = await state.get_data()
            sender_email = data['sender_email']
            sender_password = data['password']
            draft = data['draft']
            await send_emails_from_csv(sender_email, sender_password, 'Subject of your emails', draft, "default.csv")
            await message.answer("Emails have been sent successfully using the default CSV.")
            await state.clear()
        else:
            await message.answer("Please type 'upload' to upload your CSV or 'default' to use the default CSV.")
    else:
        await message.answer("Please send a text message indicating your choice.")


# Updated handler to upload a CSV file and send emails
@router.message(EmailStates.awaiting_csv_upload)
async def handle_document(message: types.Message, state: FSMContext):
    if message.document:
        document_id = message.document.file_id
        file_info = await bot.get_file(document_id)
        file_path = await bot.download_file(file_info.file_path)

        unique_filename = f"user_uploaded_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

        async with aiofiles.open(unique_filename, "wb") as f:
            await f.write(file_path.read())
            await f.close()

        data = await state.get_data()
        sender_email = data['sender_email']
        sender_password = data['password']
        draft = data['draft']
        subject = data['subject']  # Получаем сохраненную тему
        await send_emails_from_csv(sender_email, sender_password, subject, draft, unique_filename)
        await message.answer(f"Emails have been sent successfully using your uploaded CSV: {unique_filename}.")
        await state.clear()
    else:
        await message.answer("Please upload a CSV file.")


# Updated function to read email addresses from a CSV file and send emails asynchronously
async def send_emails_from_csv(sender_email, sender_password, subject, content, csv_filename):
    """Asynchronously read email addresses from a CSV file and send emails via Gmail SMTP."""
    success_count = 0
    fail_count = 0

    try:
        async with aiofiles.open(csv_filename, mode='r', encoding='utf-8') as csvfile:
            contents = await csvfile.read()
            reader = csv.reader(contents.splitlines(), delimiter=';')
            next(reader)  # Skip the header

            for row in reader:
                print(f"Processing row: {row}")
                if len(row) >= 3:
                    company_name = row[0]  # Extract the company name from the second column
                    recipient_email = row[2]    # Extract the email from the third column

                    # Replace placeholders in the email content
                    personalized_content = content.replace("[Recipient's Company]", company_name)

                    print(f"--------To: {recipient_email} Content: {personalized_content}")
                    success = send_email(sender_email, sender_password, recipient_email, subject, personalized_content)
                    if success:
                        success_count += 1
                        print(f"Email successfully sent to {recipient_email}")
                    else:
                        fail_count += 1
                else:
                    print('Incomplete row found, skipping...')

    except Exception as e:
        print(f"Error reading file or processing data: {str(e)}")

    print(f"Total emails processed: {success_count + fail_count}, Sent: {success_count}, Failed: {fail_count}")


# Function to send an email via SMTP
def send_email(sender_email, sender_password, recipient_email, subject, content):
    print("Preparing message content...")

    # Create the email message object with UTF-8 encoding
    msg = MIMEText(content, 'html', 'utf-8')

    # Set other headers with UTF-8 encoding
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = sender_email
    msg['To'] = recipient_email

    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    try:
        print("Connecting to SMTP server...")
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            print("Logging in...")
            server.login(sender_email, sender_password)
            print("Sending email...")
            server.send_message(msg)
            print(f"Email successfully sent to {recipient_email} using {smtp_server}!")
            return True
    except Exception as e:
        print(f"Failed to send email via {smtp_server}: {str(e)}")
        return False


@router.message(Command("send_answer"))
async def send_email_command(message: types.Message, state: FSMContext):
    await message.answer("Hello! Please enter your text, I will write an answer:")
    await state.set_state(AnswerStates.answer_text)


@router.message(AnswerStates.answer_text)
async def answer_text(message: types.Message, state: FSMContext):
    user_text = message.text

    # Extract email address from the user's message
    email_match = re.search(r'\b[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\b', user_text)
    if email_match:
        recipient_email = email_match.group(0)
    else:
        await message.answer("Could not find an email address in your message. Please include an email address.")
        return

    draft = await generate_answer_draft(user_text)
    await state.update_data(draft=draft, recipient_email=recipient_email)
    await message.answer(
        f"Here is the draft of your answer:\n{draft}\nIf it looks good, type 'yes' to proceed. If you need to make changes, type your corrections.")
    await state.set_state(AnswerStates.answer_draft)


async def generate_answer_draft(text):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Create a professional and polite response to the following inquiry"
                                              "Don't use placeholders!!!"
                                              "Ensure that each paragraph is separated by a blank line for clear readability."
                                              "Don't write contact information."
                                              "Don't use placeholders [Your Full Name],[Your Position],[Your Name]"},
                {"role": "user", "content": text}
            ],
            max_tokens=300
        )
        content = response.choices[0].message['content'].strip()
        # Split content into paragraphs
        paragraphs = content.split('\n\n')
        formatted_content = ''.join(f'<p>{para}</p>' for para in paragraphs)
        return formatted_content
    except Exception as e:
        print(f"Error generating answer draft: {str(e)}")
        return "Sorry, I couldn't generate a draft due to an error."


@router.message(AnswerStates.answer_draft)
async def draft_review(message: types.Message, state: FSMContext):
    response = message.text.lower()
    if response == 'yes':
        await message.answer("Please enter the email address you want to use for sending the email:")
        await state.set_state(AnswerStates.awaiting_sender_email_answer)
    else:
        await state.update_data(draft=response)
        await message.answer(
            "Draft updated. Please review and type 'yes' to send, or continue to make further corrections.")
        await state.set_state(AnswerStates.answer_correct)


@router.message(AnswerStates.awaiting_sender_email_answer)
async def handle_sender_email(message: types.Message, state: FSMContext):
    sender_email = message.text
    if is_valid_email(sender_email):
        await state.update_data(sender_email=sender_email)
        await message.answer("Sender email set. Please enter the password for SMTP authentication:")
        await state.set_state(AnswerStates.awaiting_password_answer)
    else:
        await message.answer("Please enter a valid email address.")


@router.message(AnswerStates.awaiting_password_answer)
async def handle_password(message: types.Message, state: FSMContext):
    password = message.text
    await state.update_data(password=password)

    # Proceed to send the email
    data = await state.get_data()
    draft = data['draft']
    recipient_email = data['recipient_email']
    sender_email = data['sender_email']
    sender_password = data['password']

    subject = "Response to your inquiry"
    success = send_email_answer(sender_email, sender_password, recipient_email, subject, draft)
    if success:
        await message.answer(f"Your answer has been sent to {recipient_email}.")
    else:
        await message.answer("Failed to send the email. Please try again later.")

    await state.clear()  # Clear the state


# Function to send an email via SMTP
def send_email_answer(sender_email, sender_password, recipient_email, subject, content):
    print("Preparing message content...")

    # Create the email message object with UTF-8 encoding
    msg = MIMEText(content, 'html', 'utf-8')

    # Set other headers with UTF-8 encoding
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = sender_email
    msg['To'] = recipient_email

    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    try:
        print("Connecting to SMTP server...")
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            print("Logging in...")
            server.login(sender_email, sender_password)
            print("Sending email...")
            server.send_message(msg)
            print(f"Email successfully sent to {recipient_email} using {smtp_server}!")
            return True
    except Exception as e:
        print(f"Failed to send email via {smtp_server}: {str(e)}")
        return False


def is_valid_email_answer(email):
    pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return re.match(pattern, email) is not None


def extract_code_from_message(text):
    # Extract the code parameter from the callback URL
    if "code=" in text:
        code = text.split("code=")[1].split("&")[0]
        return code
    return None


# Main function to start the bot.
async def main():
    dp = Dispatcher()
    dp.include_router(router)
    dp.include_router(router_email)
    dp.include_router(router_search)
    dp.include_router(router_answer)
    dp.include_router(router_linkedin)
    await dp.start_polling(bot)


if __name__ == '__main__':
    # Initialize the email list
    email_list = []
    asyncio.run(main())
