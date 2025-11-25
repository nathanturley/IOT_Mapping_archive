from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time


def get_offline_nodes(url, wait_time=5, headless=True):
    """
    Scrape ThingsBoard dashboard for offline nodes.
    
    Args:
        url (str): The ThingsBoard dashboard URL
        wait_time (int): Seconds to wait for page to load (default: 5)
        headless (bool): Run browser in headless mode (default: False)
    
    Returns:
        list: List of tuples containing (name, node_id) for offline nodes
    """
    # Setup Chrome options
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    
    # Additional options for running in CI/server environments
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')

    # Initialize the driver
    driver = webdriver.Chrome(options=chrome_options)

    driver.set_page_load_timeout(30) # seconds - increased for CI environments
    MAX_RETRIES = 2

    # Retry logic for loading the page
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(url)
            break  # If successful, exit the loop
        except TimeoutException:
            if attempt == MAX_RETRIES:
                print(f"Failed to load page after {MAX_RETRIES} attempts.")
                return []
            else:
                print(f"Timeout loading page, retrying ({attempt}/{MAX_RETRIES})...")

    try:

        # Wait for JavaScript to load the content
        print(f"Loading page, waiting {wait_time} seconds...")
        time.sleep(wait_time)

        # Get the rendered HTML
        soup = BeautifulSoup(driver.page_source, "html.parser")

        results = []

        # 1. Find all areas where status text is "Offline"
        offline_blocks = soup.find_all("div", class_="n_value")
        
        print(f"Found {len(offline_blocks)} status blocks")

        for block in offline_blocks:
            status_text = block.get_text(strip=True)

            if status_text == "Offline":
                # 2. Move up to the card container
                card = block.find_parent("div", class_="n_card")
                
                if not card:
                    print("Warning: Could not find parent card for offline block")
                    continue

                # 3. Extract the name (inside .m_content) - get only the first text before <br>
                name_elem = card.find("div", class_="m_content")
                # Get only the first text node (before the first <br> tag)
                name = name_elem.contents[0].strip() if name_elem and name_elem.contents else "Unknown"

                # 4. Extract the node ID (inside .n2_valueSmall)
                small = card.find("div", class_="n2_valueSmall")
                if not small:
                    print(f"Warning: Could not find node ID for {name}")
                    continue
                    
                text = small.get_text(" ", strip=True)

                # Split on "Node ID:" and extract just the ID part (before "Type:")
                if "Node ID:" in text:
                    node_id = text.split("Node ID:")[1].split("Type:")[0].strip()
                else:
                    node_id = "Unknown"

                results.append((name, node_id))
                print(f"Found offline node: {name} ({node_id})")

        print(f"Total offline nodes found: {len(results)}")
        return results
    
    finally:
        # Always close the browser
        driver.quit()


def main():
    """Main function to run the scraper standalone."""
    URL = "https://live2.innovateauckland.nz/dashboard/baafc030-dfa9-11ec-bc22-bb13277b57e1?publicId=8d688430-d497-11ec-92a2-f938b249c783"
    
    offline_nodes = get_offline_nodes(URL)
    
    # Print results
    for name, nid in offline_nodes:
        print(name, ",", nid)


if __name__ == "__main__":
    main()
