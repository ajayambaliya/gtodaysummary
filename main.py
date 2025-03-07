import os
import re
import requests
import mysql.connector
import time
import random
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from mysql.connector import Error
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from deep_translator import GoogleTranslator, MyMemoryTranslator
from googletrans import Translator
import firebase_admin
from firebase_admin import credentials, messaging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Get the absolute path of the script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# MongoDB setup
def initialize_mongodb():
    try:
        mongo_uri = os.getenv('MONGO_URI')
        if not mongo_uri:
            raise ValueError("MONGO_URI environment variable not set.")
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        db = client['current_affairs_db']
        collection = db['scraped_urls']
        return collection
    except (ConnectionFailure, Exception):
        return None

# Firebase initialization
def initialize_firebase():
    try:
        service_account_json = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON')
        if service_account_json:
            cred_dict = json.loads(service_account_json)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
            return True

        service_account_path = os.getenv('FIREBASE_SERVICE_ACCOUNT_PATH', 'service-account.json')
        path = os.path.join(SCRIPT_DIR, service_account_path) if not os.path.isabs(service_account_path) else service_account_path
        if os.path.exists(path):
            cred = credentials.Certificate(path)
            firebase_admin.initialize_app(cred)
            return True

        raise ValueError("No valid Firebase credentials found.")
    except Exception:
        return False

# Send Firebase notification
def send_firebase_notification(news_title, post_id):
    current_date = datetime.now().strftime('%d %b')
    base_title = f"{current_date} CA Summary"
    
    catchy_phrases = [
        " - Hot Updates Await!",
        " - Don’t Miss Today’s Buzz!",
        " - Fresh News Just In!",
        " - Your Daily Dose is Here!"
    ]
    
    notification_title = f"{base_title}{random.choice(catchy_phrases)}"
    
    catchy_bodies = [
        "Unveil today’s top stories in English & Gujarati! Tap now! 📰✨",
        "Stay sharp with the latest current affairs! Click to read! 🚀",
        "Your news fix is ready – dive in now! 🌟📢",
        "Big updates, small wait – tap to explore today’s summary! 🔥"
    ]
    
    try:
        topic = os.getenv('FCM_NOTIFICATION_TOPIC', 'android_news_app_topic')
        
        message = messaging.Message(
            notification=messaging.Notification(
                title=notification_title,
                body=random.choice(catchy_bodies),
            ),
            data={
                "post_id": str(post_id),
                "title": news_title,
                "click_action": "OPEN_POST"
            },
            topic=topic
        )
        response = messaging.send(message)
        print(f"Notification sent successfully. Message ID: {response}")
        return True
    except Exception as e:
        print(f"Failed to send notification: {e}")
        return False

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

class RequestsWithRetry:
    def __init__(self, retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
        self.session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def get(self, url, timeout=30):
        return self.session.get(url, timeout=timeout)

def check_and_reconnect(connection):
    try:
        if connection and connection.is_connected():
            return connection
        print("Connecting to database...")
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        print(f"Database connection error: {e}")
        return None

def insert_news(connection, cat_id, news_title, news_description, news_image):
    current_date = datetime.now().strftime('%d %B %Y')
    news_image_filename = f"{current_date} Summary.jpg"
    query = """
    INSERT INTO tbl_news (cat_id, news_title, news_date, news_description, news_image, 
                         news_status, video_url, video_id, content_type, size, view_count, last_update)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    current_timestamp = datetime.now().strftime('%Y-%m-d %H:%M:%S')
    data = (
        cat_id, news_title, current_timestamp, news_description, news_image_filename,
        1, "", "", "Post", "", 11, current_timestamp
    )
    
    try:
        connection = check_and_reconnect(connection)
        if connection:
            cursor = connection.cursor()
            cursor.execute(query, data)
            connection.commit()
            news_id = cursor.lastrowid
            cursor.close()
            print(f"News inserted successfully with ID: {news_id}")
            return True, news_id
        else:
            print("No database connection available")
            return False, None
    except mysql.connector.Error as err:
        print(f"Database insertion failed: {err}")
        print(f"Query: {query}")
        print(f"Data: {data}")
        return False, None
    except Exception as e:
        print(f"Unexpected error during insertion: {e}")
        return False, None

def log_url_to_mongodb(collection, url, status="scraped"):
    try:
        if collection is not None:
            document = {
                "url": url,
                "status": status,
                "timestamp": datetime.now(timezone.utc)
            }
            collection.insert_one(document)
    except Exception:
        pass

def is_url_scraped(collection, url):
    try:
        if collection is not None:
            result = collection.find_one({"url": url})
            return result is not None
        return False
    except Exception:
        return False

class CurrentAffairsScraper:
    def __init__(self):
        self.connection = mysql.connector.connect(**DB_CONFIG)
        self.cat_id = 1
        self.requests = RequestsWithRetry()
        self.translation_retries = 3
        self.retry_delay = 1
        self.mongodb_collection = initialize_mongodb()
        initialize_firebase()

    def clean_text(self, text):
        return re.sub(r'\s+', ' ', text).strip()

    def safe_translate(self, text, is_title=False):
        for attempt in range(self.translation_retries):
            try:
                translated_text = GoogleTranslator(source='en', target='gu').translate(text)
                return True, translated_text
            except Exception:
                try:
                    translated_text = MyMemoryTranslator(source='en', target='gu').translate(text)
                    return True, translated_text
                except Exception:
                    try:
                        translator = Translator()
                        translated_text = translator.translate(text, src='en', dest='gu').text
                        return True, translated_text
                    except Exception:
                        time.sleep(self.retry_delay)
        return False, ""

    def get_article_urls(self, page_url):
        max_retries = 3
        retry_delay = 3
        for attempt in range(max_retries):
            try:
                response = self.requests.get(page_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                articles = soup.find_all('h1', id='list')
                urls = [a.find('a')['href'] for a in articles if a.find('a')]
                if self.mongodb_collection is not None:
                    new_urls = [url for url in urls if not is_url_scraped(self.mongodb_collection, url)]
                    return new_urls
                return urls
            except requests.RequestException:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    return []
            except Exception:
                return []

    def extract_content(self, article_urls):
        articles_data = []
        total_urls = len(article_urls)
        
        for index, article_url in enumerate(article_urls, 1):
            try:
                response = self.requests.get(article_url)
                soup = BeautifulSoup(response.content, 'html.parser')
                featured_image_section = soup.find('div', class_='featured_image', style='margin-bottom:-5px;')
                
                if not featured_image_section:
                    continue
                
                title_element = soup.find('h1', id='list', style="text-align:center; font-size:20px;")
                if not title_element:
                    continue
                    
                original_title = self.clean_text(title_element.text)
                content_element = featured_image_section.find_next('p')
                if not content_element:
                    continue
                    
                original_paragraph = self.clean_text(content_element.text)
                title_success, gujarati_title = self.safe_translate(original_title, is_title=True)
                if not title_success:
                    continue
                
                para_success, gujarati_paragraph = self.safe_translate(original_paragraph)
                if not para_success:
                    continue
                
                articles_data.append((original_title, original_paragraph, gujarati_title, gujarati_paragraph))
                log_url_to_mongodb(self.mongodb_collection, article_url, status="scraped")
                time.sleep(1)
            except Exception:
                continue
        return articles_data

    def format_news_content(self, articles_data):
        current_date = datetime.now().strftime('%d %B %Y')
        html_content = f"""
        <div class="date-header">📅 {current_date}</div>
        <div class="news-container">
            <div class="header">
                <h1>Daily Current Affairs Digest</h1>
            </div>
        """
        
        for idx, (orig_title, orig_para, guj_title, guj_para) in enumerate(articles_data, 1):
            html_content += f"""
            <div class="news-card">
                <div class="topic-number">#{idx}</div>
                <div class="content-wrapper">
                    <div class="lang-section english">
                        <span class="lang-label">🇬🇧 English</span>
                        <h2>{orig_title}</h2>
                        <p>{orig_para}</p>
                    </div>
                    <div class="lang-section gujarati">
                        <span class="lang-label">🇮🇳 ગુજરાતી</span>
                        <h2>{guj_title}</h2>
                        <p>{guj_para}</p>
                    </div>
                </div>
            </div>
            """
        
        html_content += "</div>"
        html_content += self.generate_css_styles()
        return html_content

    def generate_css_styles(self):
        return """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Hind+Vadodara:wght@300;400;500;600;700&display=swap');
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
                font-family: 'Hind Vadodara', sans-serif;
            }
            .date-header {
                text-align: center;
                padding: 1rem;
                font-size: 1.5rem;
                color: #fff;
                background: linear-gradient(135deg, #ff6b6b, #4ecdc4);
                border-radius: 10px 10px 0 0;
            }
            .news-container {
                max-width: 1200px;
                margin: 0 auto;
                padding: 1rem;
                background: #f0f4f8;
                border-radius: 0 0 10px 10px;
            }
            .header {
                text-align: center;
                padding: 1.5rem;
                margin-bottom: 2rem;
                background: #2c3e50;
                color: #fff;
                border-radius: 8px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            }
            .header h1 {
                font-size: 2rem;
                font-weight: 600;
            }
            .news-card {
                background: #fff;
                border-radius: 12px;
                margin-bottom: 1.5rem;
                padding: 1.5rem;
                box-shadow: 0 5px 20px rgba(0,0,0,0.1);
                transition: all 0.3s ease;
                position: relative;
                overflow: hidden;
            }
            .news-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 8px 25px rgba(0,0,0,0.15);
            }
            .topic-number {
                position: absolute;
                top: 10px;
                left: 10px;
                background: #e74c3c;
                color: #fff;
                padding: 0.5rem 1rem;
                border-radius: 20px;
                font-size: 1rem;
                font-weight: 500;
            }
            .content-wrapper {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 1.5rem;
                margin-top: 2rem;
            }
            .lang-section {
                padding: 1rem;
                border-radius: 8px;
                background: #f8fafc;
                transition: all 0.3s ease;
            }
            .english { border-left: 5px solid #3498db; }
            .gujarati { border-left: 5px solid #e74c3c; }
            .lang-label {
                display: inline-block;
                font-size: 1rem;
                font-weight: 500;
                color: #fff;
                background: #2c3e50;
                padding: 0.3rem 0.8rem;
                border-radius: 15px;
                margin-bottom: 0.8rem;
            }
            .lang-section h2 {
                font-size: 1.3rem;
                color: #2c3e50;
                margin-bottom: 0.8rem;
                font-weight: 600;
                line-height: 1.3;
            }
            .lang-section p {
                font-size: 1rem;
                color: #34495e;
                line-height: 1.6;
            }
            @media (max-width: 768px) {
                .content-wrapper {
                    grid-template-columns: 1fr;
                    gap: 1rem;
                }
                .header h1 { font-size: 1.5rem; }
                .date-header { font-size: 1.2rem; }
                .lang-section h2 { font-size: 1.1rem; }
                .lang-section p { font-size: 0.95rem; }
                .news-card { padding: 1rem; }
            }
            @media (max-width: 480px) {
                .news-container { padding: 0.5rem; }
                .header { padding: 1rem; }
                .topic-number { font-size: 0.9rem; }
            }
        </style>
        """

    def main(self):
        try:
            base_url = "https://www.gktoday.in/current-affairs/"
            page_number = 1
            max_pages = 4
            all_urls = []
            
            while page_number <= max_pages:
                page_url = f"{base_url}page/{page_number}/"
                article_urls = self.get_article_urls(page_url)
                if article_urls:
                    all_urls.extend(article_urls)
                page_number += 1
                time.sleep(1)
            
            if all_urls:
                articles_data = self.extract_content(all_urls)
                
                if articles_data and len(articles_data) >= 3:
                    current_date = datetime.now().strftime('%d %B %Y')
                    news_title = f"{current_date} Current Affairs Summary in Gujarati"
                    news_description = self.format_news_content(articles_data)
                    
                    success, news_id = insert_news(
                        self.connection,
                        self.cat_id,
                        news_title,
                        news_description,
                        ""
                    )
                    
                    if success:
                        send_firebase_notification(news_title, news_id)
                        for url in all_urls:
                            log_url_to_mongodb(self.mongodb_collection, url, status="sent")
        except Exception as e:
            print(f"Main execution error: {e}")
        finally:
            if self.connection:
                self.connection.close()
                print("Database connection closed")

if __name__ == "__main__":
    scraper = CurrentAffairsScraper()
    scraper.main()
