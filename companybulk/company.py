from bs4 import BeautifulSoup


def parse_company_html(company_html_file):

    companies_house_id = None
    company_name = None
    current4_liabilities = None
    prev9_liabilities = None

    soup = BeautifulSoup(company_html_file, 'xml')

    for non_numeric in soup.find_all("ix:nonNumeric"):
        print(non_numeric)
        name = non_numeric.attrs['name']
        if name.endswith('UKCompaniesHouseRegisteredNumber'):
            if non_numeric.text is None:
                companies_house_id = float('nan')
            else:
                companies_house_id = non_numeric.text
        elif name.endswith('EntityCurrentLegalOrRegisteredName'):
            company_name = non_numeric.text

    for non_fraction in soup.find_all('ix:nonFraction'):
        print(non_fraction)
        name = non_fraction.attrs['name']
        if name.endswith('NetAssetsLiabilities'):
            if non_fraction.attrs['contextRef'] == "icur4":
                current4_liabilities = non_fraction.text
            elif non_fraction.attrs['contextRef'] == "iprev9":
                prev9_liabilities = non_fraction.text

    results = {
        'company_name': company_name,
        'companies_house_id': companies_house_id,
        'prev9_liabilites': prev9_liabilities,
        'current4_liabilities': current4_liabilities
    }

    return results


# view-source:http://download.companieshouse.gov.uk/en_accountsdata.html
# http://download.companieshouse.gov.uk/Accounts_Bulk_Data-2018-12-04.zip

if __name__ == '__main__':
    with open('data/Accounts_Bulk_Data-2018-12-04/Prod223_2277_00041145_20180331.html') as f:
        search_results = parse_company_html(f)
        print(search_results)
