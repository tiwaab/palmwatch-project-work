"""This file contains the code to scrape all the group members on the RSPO site"""

import time
import traceback
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class MEMBERscraper:
    """Class to scrape RSPO member companies, their subsidiaries and mills on the Universal Mill List."""

    OUTPUT_FILE_NAME = "rspo_members_raw.csv"
    BASE_URL = "https://rspo.my.salesforce-sites.com/membership/AT_SearchMember_VFPage"
    WAIT_TIMEOUT = 60
    OUTPUT_DIR = Path("/project/data/csv/")

    def __init__(self) -> None:
        """Initializing basic information for the scraper"""
        self.driver = None
        self.wait = None

    def setup_driver(self) -> webdriver.Remote:
        """Sets up the Remote WebDriver for Docker environment."""
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # Set download directory
        download_dir = str(self.OUTPUT_DIR)
        prefs = {"download.default_directory": download_dir}
        options.add_experimental_option("prefs", prefs)

        selenium_hosts = [
            "http://localhost:4444/wd/hub",
            "http://172.17.0.3:4444",
            "http://host.docker.internal:4444/wd/hub",
        ]

        last_exception = None

        for host in selenium_hosts:
            try:
                self.driver = webdriver.Remote(command_executor=host, options=options)
                self.wait = WebDriverWait(self.driver, self.WAIT_TIMEOUT)
                return self.driver
            except Exception as e:
                last_exception = e
                continue
        raise Exception(f"Failed to connect to Selenium hub: {last_exception}")

    def scrape_rspo(self) -> None:
        """Scrapes RSPO members across all pages"""
        all_data = []

        counter = 1  # Start counter to track which page is being scraped

        # Start WebDriver
        try:
            if not self.driver:
                self.setup_driver()
            self.driver.get(self.BASE_URL)
            time.sleep(5)

            while True:
                try:
                    # Wait for table rows to load
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "tr"))
                    )

                    # Parse the current page
                    print(
                        f"\rScraping has started for page {counter}", end="", flush=True
                    )
                    soup = BeautifulSoup(self.driver.page_source, "html.parser")
                    rows = soup.select("tr")
                    current_parent_data = None

                    for row in rows:
                        if "slds-hint-parent" in row.get("class", []):
                            # It's a parent company row
                            row_data = {}
                            for cell in row.select("[data-label]"):
                                label = cell.get("data-label")
                                div = cell.find("div")
                                if label and div:
                                    row_data[label] = div.get("title")

                            # Build the parent company row
                            current_parent_data = {
                                "name": row_data.get("Name"),
                                "country": row_data.get("Country"),
                                "category": row_data.get("Category"),
                                "parent_company": None,
                                "is_subsidiary": False,
                            }
                            all_data.append(current_parent_data)

                        else:
                            # It's a subsidiary row
                            group_info = row.select_one(".groupMemberInfo")
                            if group_info and current_parent_data:
                                for p in group_info.find_all("p"):
                                    sub_name = p.get_text(strip=True)
                                    sub_row = {
                                        "name": sub_name,
                                        "country": current_parent_data["country"],
                                        "category": None,
                                        "parent_company": current_parent_data["name"],
                                        "is_subsidiary": True,
                                    }
                                    all_data.append(sub_row)

                    # Wait for and find the next button
                    try:
                        next_button = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable(
                                (By.XPATH, '//*[@id="lightning"]/div[4]/button[3]')
                            )
                        )
                        if next_button.is_enabled():
                            next_button.click()
                            time.sleep(5)
                            counter += 1
                        else:
                            print("\nReached last page.")
                            break

                    except (TimeoutException, NoSuchElementException):
                        print(
                            "\nReached last page — next button missing or not clickable."
                        )
                        break

                except Exception as e:
                    print(f"\nError during scraping page {counter}: {e}")
                    break

            raw_rspo_df = pd.DataFrame(all_data)
            raw_output_path = self.OUTPUT_DIR / self.OUTPUT_FILE_NAME
            raw_output_path.parent.mkdir(parents=True, exist_ok=True)
            raw_rspo_df.to_csv(raw_output_path, index=False)

        except Exception as e:
            print(f"Error during scraping: {e}")
            traceback.print_exc()
            return None

        finally:
            self.driver.quit()
            print("Finished scraping")

    def scrape_uml(self) -> None:
        """Download the most recent UML list from the Rainforest Alliance website"""
        self.setup_driver()

        try:
            self.driver.get(
                "https://www.rainforest-alliance.org/business/certification/the-universal-mill-list/"
            )
            time.sleep(5)

            # Try to accept cookie consent if it appears
            try:
                consent_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.CLASS_NAME, "coi-consent-banner__agree-button")
                    )
                )
                consent_button.click()
                time.sleep(1)
            except Exception:
                print("No cookie banner or already accepted.")

            # Try to download the CSV file
            try:
                download_button = self.driver.find_element(
                    By.XPATH, '//a[contains(@href, ".csv")]'
                )
                print("Downloading Universal Mill List")
                download_button.click()
                time.sleep(10)  # Adjust based on network speed
                print("Finished downloading Universal Mill List")

            except Exception as e:
                print(f"Error finding or clicking download button: {e}")

        except Exception as e:
            print(f"Error navigating to UML page: {e}")

        finally:
            self.driver.quit()


if __name__ == "__main__":
    start_time = time.time()

    instance = MEMBERscraper()
    instance.scrape_rspo()
    instance.scrape_uml()

    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"Scraper finished in {duration:.2f} minutes.")
