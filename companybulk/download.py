import os
import pandas as pd

from io import BytesIO
from urllib.parse import urljoin
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup

from companybulk.company import parse_company_html
from companybulk.process import ProcessPool


class DownloadPool(ProcessPool):
    def process_item(self, item, process_id):
        [download_url, output_folder_name] = item
        output_filename = output_folder_name + '.pkl.bz2'

        if os.path.isfile(output_filename):
            print(f'Process #{process_id} skipping [{download_url}]... (already downloaded)')
        else:
            print(f'Process #{process_id} downloading [{download_url}]...')
            download_get_response = requests.get(download_url)

            print(f'Process #{process_id} extracting ZIP from downloaded [{download_url}]...')
            data_as_file = BytesIO(download_get_response.content)

            with ZipFile(data_as_file) as zip_file:
                zip_file_contents = zip_file.namelist()

                df = pd.DataFrame(columns=['company_name', 'companies_house_id', 'prev9_liabilites',
                                           'current4_liabilities'])
                for html_file_name in zip_file_contents:
                    if html_file_name.endswith('html'):
                        # print(f'Process #{process_id} extracting {html_file_name} from downloaded [{download_url}]...')
                        print(f'Process #{process_id} extracting {html_file_name}')
                        with zip_file.open(html_file_name) as company_data:
                            # print(f'Company: {html_file_name}')
                            results = parse_company_html(company_data)
                            df.loc[len(df)] = results

            # df.to_csv()
            df.to_pickle(output_filename)


def download_and_extract_company_data(output_folder, root_url):
    href_prefix = 'Accounts_Bulk_Data'

    # with DownloadPool(nb_workers=8) as download_pool:
    with DownloadPool(nb_workers=1) as download_pool:

        response = requests.get(root_url)
        soup = BeautifulSoup(response.content, 'html5lib')

        for link in soup.find_all('a'):
            href = link.get('href')

            if not href.endswith('.zip'):
                continue

            if href.startswith(href_prefix):

                download_url = urljoin(root_url, href)

                html_output_zip_file_name = os.path.join(output_folder, href)
                html_output_folder_name = os.path.splitext(html_output_zip_file_name)[0]

                if os.path.exists(html_output_folder_name):
                    print(f'[{html_output_folder_name}] already exists - skipping')
                else:
                    print(f'Queuing [{html_output_folder_name}]...')
                    download_pool.enqueue([download_url, html_output_folder_name])

            else:
                raise ValueError(f'Unhandled patent archive format: {href}')
