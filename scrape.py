from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait  # Added
from selenium.webdriver.support import expected_conditions as EC  # Added
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service  # If using webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager  # If using webdriver_manager
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
import logging
from datetime import datetime
import os
import random  # Added
from urllib.parse import urlparse  # If using URL validation
from fake_useragent import UserAgent  # If using User-Agent rotation
from retrying import retry  # If implementing retry logic

class OilGasJobScraper:
    def __init__(self, headless=True):
        # Initialize timestamp first
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Set up logging and directories
        self.setup_logging()
        self.setup_directories()
        
        # Initialize Chrome with webdriver_manager
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        if headless:
            chrome_options.add_argument('--headless=new')  # Use new headless mode
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--disable-popup-blocking')
        
        # Randomize User-Agent
        ua = UserAgent()
        chrome_options.add_argument(f'user-agent={ua.random}')
        
        try:
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            self.driver.maximize_window()
        except Exception as e:
            self.logger.critical(f"Failed to initialize Chromedriver: {e}")
            raise e
        
        # URLs and data storage
        self.base_url = "https://oilandgasjobsearch.com/jobs?title=Data+Engineer"
        
        # Initialize DataFrames with all expected columns
        self.columns = [
            'job_id', 'title', 'company', 'location', 'posted_date', 
            'job_type', 'seniority', 'salary_range', 'full_description',
            'responsibilities', 'requirements', 'benefits', 
            'company_description', 'industry', 'url', 'scrape_timestamp'
        ]
        self.jobs_df = pd.DataFrame(columns=self.columns)
        
        self.logger.info("Scraper initialized")

    def setup_directories(self):
        """Create necessary directories"""
        self.output_dir = 'job_scraping_output'
        self.csv_file = f'{self.output_dir}/data_scientist_jobs_{self.timestamp}.csv'
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs('logs', exist_ok=True)

    def setup_logging(self):
        """Set up logging configuration"""
        self.logger = logging.getLogger('JobScraper')
        self.logger.setLevel(logging.DEBUG)  # Set to DEBUG for detailed logs during troubleshooting
        
        fh = logging.FileHandler(f'logs/scraper_{self.timestamp}.log')
        ch = logging.StreamHandler()
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def is_valid_url(self, url):
        """Validate URL structure"""
        parsed = urlparse(url)
        return all([parsed.scheme, parsed.netloc, parsed.path])

    def extract_text_safely(self, element, selector, multiple=False):
        """Safely extract text from elements"""
        try:
            if multiple:
                elements = element.find_elements(By.CSS_SELECTOR, selector)
                return [el.text.strip() for el in elements if el.text.strip()]
            else:
                el = element.find_element(By.CSS_SELECTOR, selector)
                return el.text.strip()
        except:
            return None if not multiple else []

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def scrape_job_details(self, job_url=None):
        """Scrape comprehensive job details from individual job listing page"""
        try:
            if job_url:
                self.logger.info(f"Accessing job URL: {job_url}")
                self.driver.get(job_url)
                time.sleep(random.uniform(1, 3))  # Polite delay

            # Updated selectors based on provided HTML structure
            selectors = {
                'title': "div[data-testid='job-title']",
                'company': "span[data-testid='company-title-text']",
                'location': "a.JobDetail_mobileValue__LDlav[href*='/jobs/jobs-in-']",
                'posted_date': "div.JobDetail_mobileValue__LDlav:not([href])",  # Select div with class but no href
                'job_type': "a.JobDetail_mobileValue__LDlav[href*='occupationType']",
                'seniority': "a.JobDetail_mobileValue__LDlav[href*='/jobs/']",  # Last job category link
            }

            # Extract basic job information with explicit waits
            job_info = {
                'job_id': job_url.split('/')[-1] if job_url else None,
                'url': job_url
            }

            # Use WebDriverWait for each element
            wait = WebDriverWait(self.driver, 10)
            for key, selector in selectors.items():
                try:
                    element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    job_info[key] = element.text.strip()
                except TimeoutException:
                    self.logger.warning(f"Timeout waiting for {key} element")
                    job_info[key] = None
                except Exception as e:
                    self.logger.warning(f"Error extracting {key}: {e}")
                    job_info[key] = None

            # Extract job description content
            try:
                description_section = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "HtmlRenderer_renderer__mr82C"))
                )
                
                # Store full description
                job_info['full_description'] = description_section.text

                # Initialize sections
                sections = {
                    'responsibilities': [],
                    'requirements': [],
                    'benefits': []
                }

                # Process the content by paragraphs
                paragraphs = description_section.find_elements(By.TAG_NAME, 'p') + \
                            description_section.find_elements(By.TAG_NAME, 'div')

                current_section = None
                for para in paragraphs:
                    text = para.text.lower().strip()
                    
                    # Determine section based on common headers
                    if any(x in text for x in ['responsibilities', 'role', 'what you\'ll do']):
                        current_section = 'responsibilities'
                    elif any(x in text for x in ['requirements', 'qualifications', 'you must have', 'what you need']):
                        current_section = 'requirements'
                    elif any(x in text for x in ['benefits', 'we offer', 'what we provide', 'perks']):
                        current_section = 'benefits'
                    elif current_section and text:  # Add content to current section
                        sections[current_section].append(para.text.strip())

                # Convert lists to strings and add to job_info
                for section, content in sections.items():
                    job_info[section] = '\n'.join(content) if content else None

                # Extract additional metadata if available
                try:
                    metadata_list = description_section.find_element(By.TAG_NAME, 'ul')
                    metadata_items = metadata_list.find_elements(By.TAG_NAME, 'li')
                    
                    for item in metadata_items:
                        text = item.text
                        if 'JOB ID:' in text:
                            job_info['job_id'] = text.split('JOB ID:')[-1].strip()
                        elif 'Category:' in text:
                            job_info['industry'] = text.split('Category:')[-1].strip()
                except NoSuchElementException:
                    self.logger.debug("No additional metadata found")

            except TimeoutException:
                self.logger.error(f"Timeout while waiting for job description: {job_url}")
                return None
            except Exception as e:
                self.logger.error(f"Error processing job description: {e}")
                return None

            # Add salary range if found (marked as N/A in example)
            job_info['salary_range'] = self.extract_text_safely(
                self.driver,
                "div.JobDetail_mobileValue__LDlav:contains('N/A')"
            ) or None

            self.logger.info(f"Successfully scraped job: {job_info['title']}")
            return job_info

        except Exception as e:
            self.logger.error(f"Unexpected error scraping {job_url}: {e}")
            # Take a screenshot for debugging
            screenshot_path = f"logs/unexpected_error_{self.timestamp}.png"
            self.driver.save_screenshot(screenshot_path)
            self.logger.info(f"Saved screenshot to {screenshot_path}")
            return None

    def save_progress(self, job_info):
        """Save job data incrementally"""
        try:
            # Add timestamp and ensure all columns exist
            job_info['scrape_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for col in self.columns:
                if col not in job_info:
                    job_info[col] = None
            
            # Create DataFrame for new job
            new_job_df = pd.DataFrame([job_info])
            
            # Append to main DataFrame
            self.jobs_df = pd.concat([self.jobs_df, new_job_df], ignore_index=True)
            
            # Save/append to CSV
            if os.path.exists(self.csv_file):
                new_job_df.to_csv(self.csv_file, mode='a', header=False, index=False)
            else:
                new_job_df.to_csv(self.csv_file, index=False)
            
            self.logger.info(f"Saved job: {job_info['title']} - {job_info['company']}")
            
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")

    def accept_cookies(self):
        """Handle cookie consent dialog"""
        try:
            self.logger.info("Attempting to handle cookie consent")

            # Define specific selectors for the "Allow all" button and the close button
            allow_all_selector = (By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")
            close_button_selector = (By.ID, "CybotCookiebotBannerCloseButtonE2E")

            # Attempt to locate and click the "Allow all" button
            try:
                self.logger.debug("Waiting for 'Allow all' button to be clickable")
                allow_all_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(allow_all_selector)
                )
                self.driver.execute_script("arguments[0].click();", allow_all_button)
                self.logger.info("Clicked 'Allow all' button to accept cookies")
                time.sleep(random.uniform(0.5, 1.5))  # Polite delay
                return True
            except TimeoutException:
                self.logger.warning("'Allow all' button not found or not clickable within the timeout period")

            # If "Allow all" button is not found, attempt to click the close button as a fallback
            try:
                self.logger.debug("Waiting for 'Close banner' button to be clickable as a fallback")
                close_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable(close_button_selector)
                )
                self.driver.execute_script("arguments[0].click();", close_button)
                self.logger.info("Clicked 'Close banner' button to dismiss cookie consent")
                time.sleep(random.uniform(0.5, 1.0))  # Polite delay
                return True
            except TimeoutException:
                self.logger.warning("'Close banner' button not found or not clickable within the timeout period")

            # As a last resort, attempt to click any button with the class 'CybotCookiebotDialogBodyButton'
            try:
                self.logger.debug("Attempting to locate any button with class 'CybotCookiebotDialogBodyButton'")
                fallback_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "CybotCookiebotDialogBodyButton"))
                )
                self.driver.execute_script("arguments[0].click();", fallback_button)
                self.logger.info("Clicked fallback cookie consent button")
                time.sleep(random.uniform(0.5, 1.0))  # Polite delay
                return True
            except TimeoutException:
                self.logger.warning("No suitable cookie consent button found")

            # If all attempts fail, log a warning
            self.logger.warning("Could not find any cookie consent accept button")
            return False

        except Exception as e:
            self.logger.error(f"Unexpected error while handling cookie consent: {e}")
            return False

    def load_full_page(self):
        """Load the full page by scrolling if necessary"""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(1, 2))  # Polite delay
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_job_links(self):
        """Get all job links from current page"""
        job_links = []
        try:
            self.logger.info("Getting job links from current page")
            
            # First wait for page load
            time.sleep(3)  # Allow initial page load
            
            # Wait for any loading spinner to disappear (if exists)
            try:
                WebDriverWait(self.driver, 10).until_not(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='loading']"))
                )
            except:
                self.logger.debug("No loading indicator found or already disappeared")
            
            # Wait for job cards with the specific class
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".JobCard_compWrap__915q5"))
                )
            except TimeoutException:
                self.logger.error("Timeout waiting for job cards to load")
                # Take screenshot for debugging
                self.driver.save_screenshot(f"debug_screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                return []
            
            # Get all job cards
            cards = self.driver.find_elements(By.CSS_SELECTOR, ".JobCard_compWrap__915q5")
            
            if not cards:
                self.logger.warning("No job cards found after waiting")
                return []
            
            self.logger.info(f"Found {len(cards)} job cards")
            
            # Extract links from cards
            for card in cards:
                try:
                    link = card.find_element(By.CSS_SELECTOR, "a[class*='JobCard_text']").get_attribute('href')
                    if link:
                        job_links.append(link)
                        self.logger.debug(f"Found job link: {link}")
                except NoSuchElementException:
                    self.logger.warning("Could not find link in job card")
                    continue
                except Exception as e:
                    self.logger.warning(f"Error extracting link from card: {str(e)}")
                    continue
            
            self.logger.info(f"Successfully extracted {len(job_links)} job links")
            return job_links
                
        except Exception as e:
            self.logger.error(f"Error getting job links: {e}")
            return []

    def scrape_jobs(self, num_pages=389):
        """Main scraping function"""
        try:
            self.logger.info(f"Starting scraping process for {num_pages} pages")
            self.driver.get(self.base_url)
            
            # Handle cookie consent
            for _ in range(3):
                if self.accept_cookies():
                    break
                time.sleep(random.uniform(1, 2))
            
            for page in range(1, num_pages + 1):
                self.logger.info(f"Scraping page {page} of {num_pages}")
                
                # Construct page URL if needed
                if page > 1:
                    page_url = f"{self.base_url}&page={page}"
                    self.driver.get(page_url)
                    time.sleep(random.uniform(1, 2))
                
                # Get job links from current page
                job_urls = self.get_job_links()
                
                if not job_urls:
                    self.logger.warning(f"No job links found on page {page}")
                    continue

                self.logger.info(f"Found {len(job_urls)} jobs on page {page}")

                for job_url in job_urls:
                    try:
                        job_info = self.scrape_job_details(job_url)
                        if job_info:
                            self.save_progress(job_info)
                            time.sleep(random.uniform(1, 2))  # Polite delay between requests
                    except Exception as e:
                        self.logger.error(f"Error processing {job_url}: {e}")
                        continue

                self.logger.info(f"Collected {len(self.jobs_df)} jobs so far")

        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
        finally:
            self.driver.quit()
            self.logger.info("Scraping completed. Browser closed.")


    def get_results(self):
        """Return the complete DataFrame"""
        return self.jobs_df

def main():
    scraper = OilGasJobScraper()
    scraper.scrape_jobs(num_pages=389)
    
    df = scraper.get_results()
    print(f"\nScraping completed! Collected {len(df)} jobs")
    print("\nDataFrame Preview:")
    print(df.head())
    
    print(f"\nResults saved to: {scraper.csv_file}")
    print(f"Logs saved to: logs/")

if __name__ == "__main__":
    main()
