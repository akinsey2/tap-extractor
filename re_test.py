import re
from bs4 import BeautifulSoup as bs
from datetime import datetime
from time import sleep

XML_FILE_PATH = "C:\\Users\\Stephanie\\Documents\\Machine Learning\\Web Scraping\\Trade-A-Plane\\"
HTML_FILE_PATH = "C:\\Users\\Stephanie\\Documents\\Machine Learning\\Web Scraping\\Trade-A-Plane\\tap_html_files\\"
full_file_path = ''.join([XML_FILE_PATH, "sitemap_aircraft_results1.xml"])

# with open(full_file_path, mode='r', encoding="utf-8") as xml_file:
#     sitemap = xml_file.read()
#
# match = re.search(r"cat.*?=(.*?)&", sitemap)
#
# acft_types_crawled = []
# for acft_type_match in re.finditer(r"cat.*?=(.*?)&", sitemap):
#     acft_type = acft_type_match.group(1)
#     if acft_type not in acft_types:
#         print(acft_type)
#         acft_types.append(acft_type)
# print(acft_types)

with open(HTML_FILE_PATH + "tap_test_pg1_2022-09-28_10h01m28s.htm") as fp1:
    soup1 = bs(fp1, "html.parser")

select_field = soup1.find(name="select", class_="sort_options")
last_updtd_asc_sort_option = select_field.find_all(name="option",
                                                    value=re.compile("asc"),
                                                    string=re.compile("Last Updated"))
last_updtd_asc_url_str = last_updtd_asc_sort_option[0]["value"]
# Extract search parameters from URL query
all_params = last_updtd_asc_url_str.split(sep="?")[1]
params = all_params.split(sep="&")
search_params = { param.split("=")[0] : param.split("=")[1] for param in params}

print(last_updtd_asc_url_str)
if params:
    for param in params:
        print(f"'{param}'")

print(search_params)

select_results_shown = soup1.find(name="select", class_="results_shown")
max_results_shown_option = select_results_shown.find_all(name="option")[-1]
max_results = int(max_results_shown_option.string)

max_results_url_params = max_results_shown_option["value"].split("?")[-1].split("&")[-1].split("=")
search_params.update({max_results_url_params[0]: max_results_url_params[1]})


search_params.update({})
print(max_results_shown_option)
print(max_results == 96)
print(f"'{max_results_url_params}'")
print(search_params)

search_params["s-page_size"] = 97

print(search_params)

start = datetime.now()

sleep(4.6)

print(f"Time elapsed: {datetime.now() - start}")



