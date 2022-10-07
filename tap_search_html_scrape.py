# Also requires lxml library to be installed!

import requests
import re
from datetime import datetime
from urllib.parse import urlencode
import xml.etree.ElementTree as xmlET
from bs4 import BeautifulSoup as bs
from time import sleep
from random import uniform
import logging
# import boto3
# import boto3.session
# import botocore

USE_AWS = False

# Need to update every ~6 months!
USER_AGENT_STRING = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:101.0) Gecko/20100101 Firefox/101.0"
# Other constants
HTML_FILE_PATH = "C:\\Users\\Stephanie\\Documents\\Machine Learning\\Web Scraping\\Trade-A-Plane\\tap_html_files\\"
HTML_SEARCH_BUCKET_PATH = "tap_html/search_pages/"
LOG_FILE_PATH = "C:\\Users\\Stephanie\\Documents\\Machine Learning\\Web Scraping\\Trade-A-Plane\\tap_html_files\\Logs\\"
HTML_SEARCH_LOG_BUCKET_PATH = "tap_html/search_pages/logs"
AVG_REQ_DELAY = 4.0


def create_session():
    """
    Create a "requests" library Session object for maintaining a session with target server
    across individual requests

    :return: new requests.Session() object
    """

    sess1 = requests.Session()

    # Define User Agent and GET request params
    sess1.headers["Host"] = "www.trade-a-plane.com"
    sess1.headers["User-Agent"] = USER_AGENT_STRING
    sess1.headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    sess1.headers["Accept-Language"] = "en-US,en;q=0.5"
    sess1.headers["DNT"] = "1"
    sess1.headers["Sec-Fetch-Dest"] = "document"
    sess1.headers["Sec-Fetch-Mode"] = "navigate"
    sess1.headers["Sec-Fetch-Site"] = "none"
    sess1.headers["Sec-Fetch-User"] = "?1"

    return sess1


def crawl_search_url_data(sess1):
    """
    Crawls to find the proper "base" URL for TAP aircraft search
    Starts at robots.txt, finds sitemap index XML page, then aircraft sitemap XML page
    Finds and returns "base URL" for aircraft search
    Also constructs and returns list of TAP's "aircraft category" names

    :param sess1: requests.Session object for current server Session

    :return:
        str - "base" search URL,
        list of strs - aircraft types,
        list of strs - session logs
    """

    # Try to read robots.txt
    try:
        robots_resp = sess1.get("https://www.trade-a-plane.com/robots.txt")
    except Exception as err1:
        logging.error("Exception trying to reach robots.txt \n" + str(err1) +
                      "\n Assuming sitemap_index_url is 'https://www.trade-a-plane.com/sitemap_index.xml'")
        sitemap_index_url = "https://www.trade-a-plane.com/sitemap_index.xml"
    else:
        if robots_resp.status_code == 200:
            match1 = re.search(r"sitemap: (\S+)", robots_resp.text)
            sitemap_index_url = match1.group(1)
            logging.info(f"Traversing robots.txt --> Found sitemap index: '{sitemap_index_url}' -->")
        else:
            logging.warning(f"robots.txt non-standard HTTP response: {robots_resp.status_code}\n" +
                            robots_resp.text +
                            "\nAssuming sitemap_index_url is 'https://www.trade-a-plane.com/sitemap_index.xml'")
            sitemap_index_url = "https://www.trade-a-plane.com/sitemap_index.xml"

    # Try to read sitemap index XML
    try:
        sitemap_index_resp = sess1.get(sitemap_index_url)
    except Exception as err1:
        logging.error("Exception trying to reach sitemap index URL \n" + str(err1) +
                      "\nAssuming acft_sitemap_url is 'https://www.trade-a-plane.com/sitemap_aircraft_results1.xml'")
        acft_sitemap_url = "https://www.trade-a-plane.com/sitemap_aircraft_results1.xml"
    else:

        # If sitemap_index_url HTTP Response is "OK"
        if sitemap_index_resp.status_code == 200:

            # Parse XML using ElementTree library
            root = xmlET.fromstring(sitemap_index_resp.text)

            # Very annoying, must manually extract XML namespace URI string
            # ns_URI = re.match("\{.*\}", root.tag).group(0)
            # Decided not to use 'root.findall(f"{ns_URI}loc")' after all

            # Relying on "sitemap_index.xml" structure instead
            acft_sitemap_url = ""
            for sitemap in root:

                if 'aircraft' in sitemap[0].text:
                    acft_sitemap_url = sitemap[0].text
                    logging.info(f"Found aircraft sitemap: '{acft_sitemap_url}' -->")
                    break

            if not acft_sitemap_url:
                logging.error("Could not find 'aircraft' sitemap URL in sitemap index XML file." +
                              "\nAssuming 'https://www.trade-a-plane.com/sitemap_aircraft_results1.xml' -->")
                acft_sitemap_url = "https://www.trade-a-plane.com/sitemap_aircraft_results1.xml"

        # If sitemap_index_url HTTP response is NOT "OK"
        else:
            logging.warning(f"sitemap_index_url non-standard HTTP response: {sitemap_index_resp.status_code}\n"
                            + sitemap_index_resp.text +
                            "\nAssuming acft_sitemap_url is " +
                            "'https://www.trade-a-plane.com/sitemap_aircraft_results1.xml'")
            acft_sitemap_url = "https://www.trade-a-plane.com/sitemap_aircraft_results1.xml"

    search_url = ""
    acft_types_crawled = []

    # Try to read aircraft sitemap XML
    try:
        acft_sitemap_resp = sess1.get(acft_sitemap_url)
    except Exception as err1:
        logging.error("Exception trying to reach aircraft sitemap URL \n" +
                      str(err1) +
                      "Assuming search_url is 'https://www.trade-a-plane.com/search'")
        search_url = "https://www.trade-a-plane.com/search"
        acft_types_crawled = ["Single+Engine+Piston", "Multi+Engine+Piston", "Turboprop", "Jets",
                              "Gliders+|+Sailplanes", "Rotary+Wing",
                              "Piston+Helicopters", "Turbine+Helicopters", "Balloons++|++Airships"]
    else:
        # If aircraft sitemap returns HTTP response "OK"
        if acft_sitemap_resp.status_code == 200:

            # Parse XML using ElementTree library
            root = xmlET.fromstring(acft_sitemap_resp.text)

            # Find "base" search URL and populate "aircraft_types"
            for child in root:

                # Extract Search URL
                if not search_url:
                    match1 = re.search(r"https://.+\?", child[0].text)
                    if match1:
                        search_url = match1.group(0)[:-1]
                        logging.info(f"Found base search URL: '{search_url}'")

                # Populate "aircraft types" list
                match2 = re.search(r"cat.*?=(.*?)&", child[0].text)
                if match2:
                    if match2.group(1) not in acft_types_crawled:
                        acft_types_crawled.append(match2.group(1))

            if not search_url:
                logging.error("Could not find base search URL in aircraft sitemap. " +
                              "Assuming search_url = 'https://www.trade-a-plane.com/search'")
                search_url = "https://www.trade-a-plane.com/search"

            if acft_types_crawled:
                logging.info(f"Found Aircraft Categories:\n" + str(acft_types_crawled))
                # for a_t in acft_types_crawled:
                #     logging.info(f"'{a_t}'")
            else:
                acft_types_crawled = ["Single+Engine+Piston", "Multi+Engine+Piston", "Turboprop", "Jets",
                                      "Gliders+|+Sailplanes", "Rotary+Wing",
                                      "Piston+Helicopters", "Turbine+Helicopters", "Balloons++|++Airships"]
                logging.warning("Could not find Aircraft Categories.  Assuming the following categories:\n" +
                                str(acft_types_crawled))

        # If aircraft sitemap returns HTTP response NOT "OK"
        else:
            logging.warning(f"acft_sitemap_url non-standard HTTP response: {acft_sitemap_resp.status_code}\n" +
                            acft_sitemap_resp.text)
            search_url = "https://www.trade-a-plane.com/search"
            acft_types_crawled = ["Single+Engine+Piston", "Multi+Engine+Piston", "Turboprop", "Jets",
                                  "Gliders+|+Sailplanes", "Rotary+Wing",
                                  "Piston+Helicopters", "Turbine+Helicopters", "Balloons++|++Airships"]
            logging.info("Assuming search_url is 'https://www.trade-a-plane.com/search' " +
                         "with the following aircraft category names:\n" +
                         str(acft_types_crawled))

    return search_url, acft_types_crawled


def get_search_page(sess, url1, search_params):
    """
    Retrieves a single HTML search results page

    :param sess: requests.Session object for current server Session
    :param url1: string, "base" search URL without request params
    :param search_params: dict of strs, URL arguments/parameters for GET request

    :return: requests.Response object of single HTML page
    """

    # It turns out that trade-a-plane.com doesn't support the 'x-www-form-encoded' standard
    # which converts '+' into '%2B', so we need to manually encode, retaining the '+'
    search_params_enc = urlencode(search_params, safe=':+')

    # Construct Request Object
    req = requests.Request(method='GET', url=url1, params=search_params_enc)
    prep_req = sess.prepare_request(req)

    try:
        response = sess.send(prep_req)
    except Exception as err1:
        logging.error(f"Exception trying to reach page #{search_params['s-page']} at '{prep_req.url}'\n" + str(err1))
        response = False
    else:
        if response.status_code == 200:
            logging.info(f"Retrieved page #{search_params['s-page']} successfully at '{prep_req.url}'")
        else:
            logging.warning(f"Error: when sending request: '{prep_req.url}'\n" +
                            f"received non-standard HTML response: {response.status_code}\n" +
                            response.text)

    return response


def write_html_file(response, search_params, acft_type=""):
    """
    Writes a retrieved 'requests.Response' object to an HTML file on disk

    :param response: requests.Response object of HTML page
    :param search_params: dict of strs, URL arguments/parameters for GET request
    :param sess_logs: Python list for temp storage of logging statements
    :param acft_type: string, one of TAP's aircraft categories, if desired

    :return: None
    """

    acft_type_str = re.sub(r"[^A-Za-z0-9]", "", acft_type)
    now = datetime.now()
    time_str1 = now.strftime("%Y-%m-%d_%Hh%Mm%Ss")

    filename = ''.join(["tap_search_", search_params["s-type"], "_", acft_type_str,
                        "_pg", str(search_params["s-page"]), "_", time_str1, ".htm"])
    full_file_path = ''.join([HTML_FILE_PATH, filename])

    resp_enc = response.encoding.lower()

    # ADD error handling
    with open(full_file_path, mode='w', encoding=resp_enc) as file1:
        try:
            chars_written = file1.write(response.text)
        except Exception as err1:
            logging.error(f"Error writing {filename} at {time_str1}.  Details:\n" +
                          str(err1))
        else:
            logging.info(f"Wrote {full_file_path} to file")
    assert chars_written > 0

    # DEBUG
    print(f"Wrote {full_file_path} to file")

    # FINISH AWS CALL
    if USE_AWS:
        # aws_success = upload_to_S3(HTML_FILE_PATH, filename)
        pass


def upload_to_s3(path, filename):
    """ Take a saved file and upload to S3

    :param path: str, path in local filesystem
    :param filename: str
    :return: bool, True if Successful, False otherwise
    """

    # FINISH THIS FUNCTION
    try:
        pass
    except botocore.exceptions.ParamValidationError as err:
        pass
        return False
    except botocore.exceptions.ClientError as err:
        pass
        return False
    else:
        pass
        return True


def num_of_posts(response):
    """
    Extract the total current number current  search results from HTML
    :param response: 'requests.Response' object
    :return: int - number of search results found
    """

    match = re.search(r"(\d*),?(\d+)\s+results found", response.text)
    num_listings = int(match.group(1) + match.group(2))  # Convert string(s) to int
    return num_listings


def scrape_tap_search_html():
    """
    Primary function to retrieve all TAP aircraft search result pages and save as HTML files

    :return:
    """
    start_time = datetime.now()
    wait_time_total = 0
    sess_logs = []

    now = datetime.now()
    time_str1 = now.strftime("%Y-%m-%d_%HH%MM%SS")
    logging.info("Scraping TAP HTML v1 begun " + time_str1)

    # Create Requests Session
    try:
        sess1 = create_session()
    except Exception as err1:
        logging.error("Exception trying to create Requests Session.\n" +
                      str(err1))
        return
    else:
        logging.info("Requests Session Created.")

    # Get "root" search URL
    search_url, acft_types_crawled = crawl_search_url_data(sess1)

    wait = AVG_REQ_DELAY + uniform(-(AVG_REQ_DELAY/2), AVG_REQ_DELAY/2)
    wait_time_total = wait_time_total + wait
    sleep(wait)

    # Retrieve first page

    search_params = {"s-type": "aircraft", "s-page": 1}
    tap_resp1 = get_search_page(sess1, search_url, search_params)

    # Parse HTML using BeautifulSoup library
    soup1 = bs(tap_resp1.text, "lxml")

    # Get Sort_key and sort-order options programmatically
    select_sort_options = soup1.find(name="select", class_="sort_options")
    last_updtd_asc_sort_option = select_sort_options.find_all(name="option",
                                                              value=re.compile("asc"),
                                                              string=re.compile("Last Updated"))
    last_updtd_asc_url_str = last_updtd_asc_sort_option[0]["value"]
    all_params = last_updtd_asc_url_str.split(sep="?")[1]
    params = all_params.split(sep="&")
    # Combine into dict
    search_params = {param.split("=")[0]: param.split("=")[1] for param in params}

    # Get max page size programmatically
    select_results_shown = soup1.find(name="select", class_="results_shown")
    max_results_shown_option = select_results_shown.find_all(name="option")[-1]
    max_res_per_page = int(max_results_shown_option.string)

    max_results_url_params = max_results_shown_option["value"].split("?")[-1].split("&")[-1].split("=")
    search_params.update({max_results_url_params[0]: max_results_url_params[1]})

    search_params.update({"s-page": 1})

    logging.info("Found the following search parameters:" + str(search_params))
    total_results = num_of_posts(tap_resp1)
    logging.info(f"Total results found: {total_results}")

    # Iterate to retrieve and store all HTML search pages
    current_page = 1
    while (current_page*max_res_per_page - total_results) < max_res_per_page:
        wait = AVG_REQ_DELAY + uniform(-(AVG_REQ_DELAY/2), AVG_REQ_DELAY/2)
        wait_time_total = wait_time_total + wait
        sleep(wait)

        search_params["s-page"] = current_page

        page_resp = get_search_page(sess1, search_url, search_params)

        write_html_file(page_resp, search_params)

        # Update for next iteration
        total_results = num_of_posts(page_resp)
        current_page = current_page + 1

    sess1.close()   # Close adapter and connection session
    logging.info(f"Scraped and Saved {current_page-1} search page results")
    logging.info(f"Total Wait Time: {wait_time_total:.3f} (seconds)")
    total_time = datetime.now() - start_time
    logging.info(f"Total Time Elapsed: {total_time} (hours:min:secs)")

    print(f"SUCCESS: TAP HTML scrape of {current_page-1} pages in {total_time} (hours:min:secs).")

    return


if __name__ == '__main__':

    if USE_AWS:
        try:
            aws_sess = boto3.session.Session()
        except botocore.exceptions.ParamValidationError as err:
            # FINISH ERROR HANDLING
            pass
        except botocore.exceptions.ClientError as err:
            # FINISH ERROR HANDLING
            pass

    time_str = datetime.now().strftime("%Y-%m-%d_%HH%MM%SS")
    logs_filename = ''.join(["tap_html_search_scrape_log_", time_str, ".txt"])
    logs_full_file_path = ''.join([LOG_FILE_PATH, logs_filename])

    logging.basicConfig(filename=logs_full_file_path,
                        filemode="a",
                        style="{",
                        datefmt="%Y-%m-%d %H:%M:%S UTC%z",
                        format="{asctime} {levelname} {message}",
                        level=logging.DEBUG
                        )

    scrape_tap_search_html()
