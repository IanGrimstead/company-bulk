import argparse

from companybulk.download import download_and_extract_company_data


def main():
    parser = argparse.ArgumentParser(description="Downloads company accounts from companies house")

    parser.add_argument("output_folder_name", metavar='<output folder name>',
                        help="folder name where sub-folders will be created (based on publication date) and XML"
                             " files will be downloaded into.")

    args = parser.parse_args()

    root_url = 'http://download.companieshouse.gov.uk/en_accountsdata.html'

    download_and_extract_company_data(args.output_folder_name, root_url)


if __name__ == '__main__':
    main()
