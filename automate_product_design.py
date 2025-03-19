# -*- coding: utf-8 -*-
import os
import logging
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(level=logging.INFO)

# ================================
# Configuration and Credentials
# ================================

# Retrieve credentials from environment variables or use defaults
GLOBUS_USER = os.getenv("GLOBUS_USER", "won103203@naver.com")
GLOBUS_PW = os.getenv("GLOBUS_PW", "goTeamC@1")

# Path to Microsoft Edge WebDriver
edge_driver_path = "C:/Users/yuhan/Downloads/edgedriver_win64/msedgedriver.exe"

# Edge options
edge_options = Options()
# Uncomment the following line to run in headless mode (recommended for automation)
edge_options.add_argument('--headless')
edge_options.add_argument('--no-sandbox')
edge_options.add_argument('--disable-dev-shm-usage')

# ================================
# Selenium Helper Functions
# ================================

def create_edge_driver():
    """Creates and returns a new Edge WebDriver instance with the specified options."""
    service = Service(edge_driver_path)
    driver = webdriver.Edge(service=service, options=edge_options)
    driver.implicitly_wait(5)  # Set an implicit wait
    return driver

def login_to_globus(driver, user=GLOBUS_USER, password=GLOBUS_PW):
    """
    Logs in to the Glo-Bus website using the provided driver and credentials.
    Adjust the element selectors to match the actual site's login form.
    """
    try:
        driver.get("https://www.glo-bus.com/")
        # Wait for the login button or form to load (update the selector as needed)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "loginButton"))
        )
        logging.info("Login page loaded.")
        
        # Selectors for the username and password fields
        username_field = driver.find_element(By.ID, "acct_name")
        password_field = driver.find_element(By.ID, "passwdInput")
        username_field.clear()
        username_field.send_keys(user)
        password_field.clear()
        password_field.send_keys(password)
        
        # Click the login submit button
        driver.find_element(By.ID, 'loginbutton').click()
        
        # Wait for a post-login element to confirm success (update selector)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "tn btn-dec-rpt fw-bold text-white btn-primary"))
        )
        logging.info("Login successful.")
    except Exception as e:
        logging.error("Error during login: %s", e)
        raise

def open_DnR(subsection = None):
    # 로그인이 되어있지 않다면 로그인
    if not logged_in:
        login_to_glo_bus()

    # subsection이 None이 아니라면 해당 subsection으로 이동
    if subsection:
        driver.get(f'https://www.glo-bus.com/users/program21/decisions/{subsection}')
        print(f'{subsection} 페이지로 이동완료')
    else:
        # Decision and Reports 페이지로 이동
        driver.get('https://www.glo-bus.com/users/program21/')
        print('Decision and Reports 페이지로 이동완료')

def process_row(row, driver):
    """
    Processes a single row representing a product design option.
    Navigates to the corresponding page and retrieves the Performance/Quality Rating to
    unit production cost ratio.
    
    Adjust the URL structure, wait conditions, and element selectors as needed.
    """
    try:
        # Construct the URL for the design option (modify as needed)
        design_option = row.get('design_option')
        design_url = f"https://www.glo-bus.com/design?option={design_option}"
        driver.get(design_url)
        
        # Wait for the element that contains the metric (update selector)
        metric_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "performanceQualityMetric"))
        )
        metric_text = metric_element.text
        
        # Convert the extracted text to a float (adapt conversion if needed)
        metric_value = float(metric_text)
        return metric_value
    except Exception as e:
        logging.error("Error processing row %s: %s", row, e)
        return None

# ================================
# Dask Processing Functions
# ================================

def process_partition(df_partition):
    """
    Processes a single partition of the Dask DataFrame.
    This function creates its own Selenium driver instance, logs in,
    and then processes each row to compute the desired metric.
    """
    driver = create_edge_driver()
    try:
        login_to_glo_bus(driver)
        # Apply process_row to each row in the partition
        df_partition['metric'] = df_partition.apply(lambda row: process_row(row, driver), axis=1)
        return df_partition
    finally:
        driver.quit()

# ================================
# Main Execution
# ================================

def main():
    # Load your design options dataset (ensure the file exists and is correctly formatted)
    input_file = "design_options.csv"  # Update the file path as needed
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        logging.error("Error reading CSV file: %s", e)
        return

    # Create a Dask DataFrame; adjust npartitions based on your hardware resources
    ddf = dd.from_pandas(df, npartitions=4)
    
    # Process partitions in parallel using map_partitions
    result_ddf = ddf.map_partitions(process_partition)
    result_df = result_ddf.compute()

    # Identify the best design option based on the computed metric
    if result_df['metric'].notnull().any():
        best_design = result_df.loc[result_df['metric'].idxmax()]
        logging.info("Best design configuration:")
        print(best_design)
    else:
        logging.warning("No valid metric values found.")

    # Optionally, save the results to a CSV file
    try:
        result_df.to_csv("design_results.csv", index=False)
        logging.info("Results saved to design_results.csv")
    except Exception as e:
        logging.error("Error saving results: %s", e)

if __name__ == "__main__":
    main()
