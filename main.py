import os
import re
import requests
import mysql.connector
import logging
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
    """Initialize MongoDB connection."""
    try:
        mongo_uri = os.getenv('MONGO_URI')
        if not mongo_uri:
            raise ValueError("MONGO_URI environment variable not set.")
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        db = client['current_affairs_db']
        collection = db['scraped_urls']
        logging.info("MongoDB initialized successfully")
        return collection
    except ConnectionFailure as e:
        logging.error(f"MongoDB connection failed: {e}")
        return None
    except Exception as e:
        logging.error(f"MongoDB initialization error: {e}")
        return None


# Firebase initialization
def initialize_firebase():
    """Initialize Firebase with credentials from environment variable or file."""
    try:
        service_account_json = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON')
        if service_account_json:
            cred_dict = json.loads(service_account_json)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
            logging.info("Firebase initialized with service account JSON from environment")
            return True

        service_account_path = os.getenv('FIREBASE_SERVICE_ACCOUNT_PATH', 'service-account.json')
        path = os.path.join(SCRIPT_DIR, service_account_path) if not os.path.isabs(service_account_path) else service_account_path
        if os.path.exists(path):
            cred = credentials.Certificate(path)
            firebase_admin.initialize_app(cred)
            logging.info(f"Firebase initialized with service account at: {path}")
            return True

        raise ValueError("No valid Firebase credentials found. Set FIREBASE_SERVICE_ACCOUNT_JSON or provide service-account.json.")
    except Exception as e:
        logging.error(f"Firebase initialization error: {e}")
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
        logging.info(f"Notification sent successfully. Message ID: {response}")
        return True
    except Exception as e:
        logging.error(f"Failed to send notification: {e}")
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
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        logging.error(f"Connection error: {e}")
        return None

def insert_news(connection, cat_id, news_title, news_description, news_image):
    current_date = datetime.now().strftime('%d %B %Y')
    news_image_filename = f"{current_date} Summary.jpg"
    query = """
    INSERT INTO tbl_news (cat_id, news_title, news_date, news_description, news_image, 
                         news_status, video_url, video_id, content_type, size, view_count, last_update)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
            return True, news_id
        return False, None
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        return False, None

def log_url_to_mongodb(collection, url, status="scraped"):
    """Log URL to MongoDB with status and timestamp."""
    try:
        if collection is not None:
            document = {
                "url": url,
                "status": status,
                "timestamp": datetime.now(timezone.utc)
            }
            collection.insert_one(document)
            logging.info(f"Logged URL to MongoDB: {url} with status {status}")
    except Exception as e:
        logging.error(f"Failed to log URL to MongoDB: {e}")

def is_url_scraped(collection, url):
    """Check if URL has been scraped before."""
    try:
        if collection is not None:
            result = collection.find_one({"url": url})
            logging.debug(f"Checking URL {url} in MongoDB: {'Found' if result else 'Not found'}")
            return result is not None
        return False
    except Exception as e:
        logging.error(f"Error checking URL in MongoDB: {e}")
        return False

class CurrentAffairsScraper:
    def __init__(self):
        self.connection = mysql.connector.connect(**DB_CONFIG)
        self.cat_id = 1
        self.requests = RequestsWithRetry()
        self.translation_retries = 3
        self.retry_delay = 1
        self.mongodb_collection = initialize_mongodb()
        if not initialize_firebase():
            logging.error("Firebase initialization failed. Notifications will not be sent.")
        if self.mongodb_collection is None:
            logging.error("MongoDB initialization failed. Duplicate checking will be disabled.")

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
                    except Exception as e:
                        logging.warning(f"Translation attempt {attempt + 1} failed: {e}")
                        time.sleep(self.retry_delay)
        logging.error(f"Translation failed after {self.translation_retries} attempts")
        return False, ""

    def get_article_urls(self, page_url):
        max_retries = 3
        retry_delay = 3
        for attempt in range(max_retries):
            try:
                logging.debug(f"Fetching URL: {page_url}")
                response = self.requests.get(page_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                articles = soup.find_all('h1', id='list')  # Reverted to old working method
                urls = [a.find('a')['href'] for a in articles if a.find('a')]
                logging.debug(f"Raw URLs found on {page_url}: {urls}")
                
                # Filter out already scraped URLs
                if self.mongodb_collection is not None:
                    new_urls = [url for url in urls if not is_url_scraped(self.mongodb_collection, url)]
                    logging.info(f"After filtering, {len(new_urls)} new URLs remain on {page_url}: {new_urls}")
                    return new_urls
                return urls
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Attempt {attempt + 1} failed for {page_url}: {e}")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"Failed to fetch URLs after {max_retries} attempts: {e}")
                    return []
            except Exception as e:
                logging.error(f"Unexpected error fetching {page_url}: {e}")
                return []

    def extract_content(self, article_urls):
        articles_data = []
        total_urls = len(article_urls)
        
        for index, article_url in enumerate(article_urls, 1):
            logging.info(f"Processing article {index}/{total_urls}: {article_url}")
            try:
                response = self.requests.get(article_url)
                soup = BeautifulSoup(response.content, 'html.parser')
                featured_image_section = soup.find('div', class_='featured_image', style='margin-bottom:-5px;')
                
                if not featured_image_section:
                    logging.warning(f"No featured image section found in {article_url}")
                    continue
                
                title_element = soup.find('h1', id='list', style="text-align:center; font-size:20px;")
                if not title_element:
                    logging.warning(f"No title found in {article_url}")
                    continue
                    
                original_title = self.clean_text(title_element.text)
                content_element = featured_image_section.find_next('p')
                if not content_element:
                    logging.warning(f"No content found in {article_url}")
                    continue
                    
                original_paragraph = self.clean_text(content_element.text)
                title_success, gujarati_title = self.safe_translate(original_title, is_title=True)
                if not title_success:
                    logging.error(f"Failed to translate title for article {index}")
                    continue
                
                para_success, gujarati_paragraph = self.safe_translate(original_paragraph)
                if not para_success:
                    logging.error(f"Failed to translate paragraph for article {index}")
                    continue
                
                articles_data.append((original_title, original_paragraph, gujarati_title, gujarati_paragraph))
                log_url_to_mongodb(self.mongodb_collection, article_url, status="scraped")
                logging.info(f"Successfully processed article {index}/{total_urls}")
                time.sleep(1)
            except Exception as e:
                logging.error(f"Failed to process {article_url}: {e}")
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
                logging.info(f"Processed page {page_number}/{max_pages}")
                if article_urls:
                    all_urls.extend(article_urls)
                page_number += 1
                time.sleep(1)
            
            if all_urls:
                logging.info(f"Found {len(all_urls)} articles to process")
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
                        logging.info(f"Successfully created post for {current_date} with ID: {news_id}")
                        if send_firebase_notification(news_title, news_id):
                            logging.info("Notification sent successfully")
                            for url in all_urls:
                                log_url_to_mongodb(self.mongodb_collection, url, status="sent")
                        else:
                            logging.warning("Notification failed to send")
                    else:
                        logging.error(f"Failed to create post for {current_date}")
                else:
                    logging.error("Insufficient translated articles")
            else:
                logging.warning("No new articles found to process across all pages")
                
        except Exception as e:
            logging.error(f"Main execution error: {e}")
        finally:
            if self.connection:
                self.connection.close()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,  # Keep DEBUG level for troubleshooting
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    scraper = CurrentAffairsScraper()
    scraper.main()
