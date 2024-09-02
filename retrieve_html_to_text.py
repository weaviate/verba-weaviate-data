import html2text
from markdownify import markdownify as md
from bs4 import BeautifulSoup
import re
from textwrap import dedent
from urllib.parse import urljoin
import requests
import asyncio
from pyppeteer import launch
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import aiohttp
from aiohttp import ClientSession


async def get_markdown_from_url(url: str):

    html = await get_html(url)

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Remove all nav tags and their content
    for nav in soup.find_all("nav"):
        nav.decompose()

    # Remove the footer tag and its content
    for footer in soup.find_all("footer"):
        footer.decompose()

    h = html2text.HTML2Text()
    h.ignore_links = True
    h.skip_internal_links = True

    html_text = h.handle(str(soup))

    return html_text


def get_href_from_homepage(url: str = "https://weaviate.io/developers/weaviate"):
    html = get_html_selenium(url)

    soup = BeautifulSoup(html, "html.parser")
    hrefs = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/developers"):
            full_url = urljoin(url, href)
            hrefs.add(full_url)
        elif href.startswith("https://weaviate.io/developers"):
            hrefs.add(href)

    return hrefs


async def get_html(url: str):
    try:
        async with aiohttp.ClientSession() as session:
            # Send an HTTP GET request to the specified URL
            async with session.get(url) as response:
                # Check if the request was successful (status code 200)
                response.raise_for_status()

                # Extract the raw HTML content
                return await response.text()

    except aiohttp.ClientError as e:
        # Handle errors that occur during the request
        print(f"An error occurred: {e}")


async def get_html_js(url: str):
    browser = await launch(headless=True)
    page = await browser.newPage()
    await page.goto(url)
    content = await page.content()
    await page.close()
    await browser.close()
    return content


def get_html_selenium(url: str):
    # Set options for the WebDriver, for example, to run in headless mode
    options = Options()
    options.add_argument("user-agent=whatever you want")
    options.headless = True

    # Initialize the WebDriver with automatic ChromeDriver management
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # Navigate to the URL
    driver.get(url)

    # Wait for 2 seconds to ensure all content is loaded
    time.sleep(2)

    # Get the rendered HTML
    html_content = driver.page_source

    # Close the driver
    driver.quit()

    return html_content


async def recursive_get_hrefs(
    base_url: str = "https://weaviate.io/developers/weaviate",
):
    visited = set()
    to_visit = {base_url}

    async with ClientSession() as session:
        while to_visit:
            url = to_visit.pop()
            if url in visited:
                continue

            print(f"Visiting: {url}")
            try:
                async with session.get(url) as response:
                    html = await response.text()
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                continue

            visited.add(url)
            yield url

            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                full_url = urljoin(base_url, href)

                if (
                    full_url.startswith(base_url)
                    and "/developers" in full_url
                    and not "#" in full_url
                ):
                    if full_url not in visited and full_url not in to_visit:
                        to_visit.add(full_url)
                        yield full_url

            await asyncio.sleep(0.5)
