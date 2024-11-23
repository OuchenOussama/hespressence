import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

class HespressScraper:
    def __init__(self):
        self.base_url = "https://www.hespress.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def get_topic_from_url(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            breadcrumb = soup.find('ol', class_='breadcrumb')
            if breadcrumb:
                topic_li = breadcrumb.find_all('li')[1]
                if topic_li:
                    return topic_li.text.strip()
        except Exception as e:
            print(f"Error getting topic from {url}: {str(e)}")
        
        return 'unknown'

    def get_comments(self, article_url, article_title):
        try:
            response = requests.get(article_url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Add debug print
            print(f"Fetching comments for: {article_url}")
            
            # Find the comments section
            comments_list = soup.find('ul', class_='comment-list')
            if not comments_list:
                print(f"No comments list found for article: {article_url}")
                return []

            # Add debug print
            comment_items = comments_list.find_all('li', class_='comment')
            print(f"Found {len(comment_items)} comments")

            comments = []
            topic = self.get_topic_from_url(article_url)
            
            # Find all comment items
            for comment in comment_items:
                try:
                    # Get comment text
                    comment_text = comment.find('div', class_='comment-text')
                    if not comment_text:
                        print(f"No comment text found for comment in article: {article_url}")
                        continue
                        
                    comment_text = comment_text.text.strip()
                    if not comment_text:
                        print(f"Empty comment text found in article: {article_url}")
                        continue
                    
                    # Get comment score
                    score_element = comment.find('span', class_='comment-recat-number')
                    score = 0  # default value
                    if score_element:
                        try:
                            score = int(score_element.text.strip())
                        except ValueError:
                            print(f"Could not parse score: {score_element.text}")
                    
                    # Get comment ID from the li element's id attribute
                    comment_id = comment.get('id', '').replace('comment-', '')
                    
                    comment_data = {
                        'id': comment_id,
                        'comment': comment_text,
                        'topic': topic,
                        'article_title': article_title,
                        'article_url': article_url,
                        'score': score 
                    }
                    comments.append(comment_data)
                    
                except Exception as e:
                    print(f"Error processing comment: {str(e)}")
                    continue

            return comments

        except Exception as e:
            print(f"Error scraping comments from {article_url}: {str(e)}")
            return []

    def get_articles_from_website(self):
        articles = []
        
        # Setup Chromium options
        options = Options()
        
        chromium_path = "/snap/bin/chromium"
        
        options.binary_location = chromium_path
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        try:
            driver = webdriver.Chrome(options=options)
        except Exception as e:
            options.binary_location = ""
            driver = webdriver.Chrome(options=options)
        
        try:
            driver.get(f"{self.base_url}/all")
            
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            scroll_attempts = 2
            articles_seen = set()
            
            for i in range(scroll_attempts):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                time.sleep(4)
                
                article_cards = driver.find_elements(By.CSS_SELECTOR, 'div.col-12.col-sm-6.col-md-6.col-xl-3')
                
                for card in article_cards:
                    try:
                        link_element = card.find_element(By.CSS_SELECTOR, 'a.stretched-link')
                        article_url = link_element.get_attribute('href')
                        
                        if article_url not in articles_seen:
                            # Get the title
                            title = card.find_element(By.CSS_SELECTOR, 'h3.card-title')
                            title_text = title.text.replace('"', '').replace('"', '').replace('"', '').strip()
                            
                            articles.append({
                                'title': title_text,
                                'url': article_url
                            })
                            articles_seen.add(article_url)
                            
                    except Exception as e:
                        print(f"Error processing article card: {str(e)}")
                        continue
                
                print(f"Scroll {i+1}/{scroll_attempts}: Found {len(articles)} unique articles")
                
                # Calculate new scroll height
                new_height = driver.execute_script("return document.body.scrollHeight")
                
                # Break if no new content loaded
                if new_height == last_height:
                    print("No more new content loading...")
                    break
                    
                last_height = new_height
                
        except Exception as e:
            print(f"Error fetching articles from website: {str(e)}")
        
        finally:
            driver.quit()
            print(f"Total unique articles found: {len(articles)}")
        
        return articles

    def scrape_comments(self):
        """Main function to scrape comments"""
        # Get articles from website instead of RSS feed
        articles = self.get_articles_from_website()
        print(f"Found {len(articles)} articles")
        
        all_comments = []
        for article in articles:
            print(f"Scraping comments from: {article['title']}")
            comments = self.get_comments(article['url'], article['title'])
            if comments:
                all_comments.extend(comments)
            time.sleep(1) 

        # Create DataFrame
        if not all_comments:
            return pd.DataFrame(columns=['id', 'comment', 'topic', 'article_title', 'article_url', 'score'])

        df = pd.DataFrame(all_comments)
        
        # Reorder columns
        columns = ['id', 'comment', 'topic', 'article_title', 'article_url', 'score']
        if all(col in df.columns for col in columns):
            df = df[columns]
        
        return df

def main():
    scraper = HespressScraper()
    comments_df = scraper.scrape_comments()
    
    # Save to CSV file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'comments_all_vote_{timestamp}.csv'
    comments_df.to_csv(filename, index=False, encoding='utf-8-sig')
    
    print(f"Scraping completed! Data saved to {filename}")
    print(f"Total comments collected: {len(comments_df)}")

if __name__ == "__main__":
    main()
