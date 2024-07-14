"""The script for extracting and transforming the data"""

from typing import List
import requests
import polars as pl
from bs4 import BeautifulSoup

def scraper(src_url: str) -> List[str]:
    """
        Summary:
            The function that is used for scrapping (extract) from a website

        Args:
            src_url (str): The url or link of the webpage

        Returns:
            List[str]: Returns a list that represents the whole table
            
        Note:
            You could modify this function to add parameters for the class and id
            selectors of the table of the webpage that will be scraped. It is to make
            this function a lot more general and usable.
    """
    page = requests.get(src_url, timeout=10)
    soup = BeautifulSoup(page.text, 'html')

    return soup.find('table', id="pokedex")

def get_header(lst: List[str] | None = None) -> List[str] | str:
    """
        Summary:
            The function for getting the column headers from a beatiful soup table

        Args:
            lst (List[str] | None, optional): A html table produced by beautiful soup.
                                              Defaults to None.

        Returns:
            List[str] | str: Returns a list of column headers or a string
                             that signifies an empty list was given.
    """

    if lst is None:
        return "Empty parameter"

    header = lst.find('thead')
    head = header.find_all('div')

    return [tag.text for tag in head]

def get_data(dataframe_headers: pl.DataFrame, table: List[str]) -> pl.DataFrame:
    """
        Summary:
            This function creates a new polars data frame from all of the 
            table data that is cleaned and transformed

        Args:
            dataframe_headers (pl.DataFrame): Dataframe column headers
            table (List[str]): The list of uncleaned <td> from a scraped table

        Returns:
            pl.DataFrame: The cleaned dataframe with correct headers
    """
    col_data = table.find_all('tr')
    new_rows = []

    for row in col_data[1:]:
        row_data = row.find_all('td')
        indiv_row_data = [data.text.strip() for data in row_data]

        new_rows.append(indiv_row_data)

    data_df = pl.DataFrame(new_rows, schema=dataframe_headers.columns)
    return data_df

if __name__ == "__main__":
    html_table = scraper("https://pokemondb.net/pokedex/all")
    html_table_titles = get_header(html_table)
    df = pl.DataFrame(schema=html_table_titles)
    pokedex = get_data(df, html_table)
    pokedex.write_csv("scraped_pokedex.csv")
