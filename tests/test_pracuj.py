import unittest
import numpy as np
import src.pracuj as pracuj
import src.utils as utils
from selenium import webdriver
import pandas as pd
import copy

class PracujTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.df = pracuj.scrap_links(max_pages=2)
        self.cleaned_df = pracuj.remove_duplicates(self.df)

        index_data = copy.deepcopy(utils.DEFAULT_INDEX_TEMPLATE)
        driver = webdriver.Firefox()
        top_rows = self.cleaned_df['url'].head(3)
        for index, row in enumerate(top_rows):
            pracuj.extract_posting_data(driver, index_data, row, index)
        self.iterated_dict = index_data
        self.iterated_df = pd.DataFrame.from_dict(index_data)
        driver.close()


    def test_return_type_links(self):
        self.assertIsInstance(self.df, pd.DataFrame)


    def test_len_records_links(self):
        self.assertGreater(self.df.shape[0], 0)

        
    def test_nan_records_links(self):
        self.assertTrue(self.df.dropna().shape[0] == self.df.shape[0])


    def test_columns_records_links(self):
        self.assertEqual(self.df.shape[1], 4)


    def test_columns_types_records_links(self):
        for index, row in self.df.iterrows():
            self.assertIsInstance(row["id"], np.int64)
            self.assertIsInstance(row["url"], str)
            self.assertIsInstance(row["created_at"], pd.Timestamp)
            self.assertIsInstance(row["source"], str)


    def test_columns_values_records_links(self):
        for index, df_id in enumerate(self.df['id']):
            self.assertEqual(df_id, index)
        for source in self.df['source']:
            self.assertEqual(source, 'pracuj')
        for url in self.df['url']:
            self.assertTrue('pracuj.pl' in url, f'Url is {url}')


    def test_remove_duplicates_links(self):
        test_data = {
            "id": [0, 1, 2],
            "url": ["https://www.pracuj.pl/praca/talent-sourcing-specialist-lodz", "https://www.pracuj.pl/praca/talent-sourcing-specialist-lodz", "https://www.pracodawcy.pracuj.pl/praca/stazysta-specjalista-ds-monitoringu-systemow-it-poznan-szarych-szeregow-23"],
            "created_at": ["2024-09-15 19:58:58.102488", "2024-09-15 19:58:58.102488", "2024-09-15 19:58:58.102488"],
            "source": ["pracuj", "pracuj", "pracuj"]
        }
        test_df = pd.DataFrame.from_dict(test_data)
        removed = pracuj.remove_duplicates(test_df)
        self.assertEqual(removed.shape[0], 1)


    def test_dict_posting_data(self):
        id_count = 0
        for key, value in self.iterated_dict.items():
            id_count = len(value)
            self.assertEqual(len(value), id_count)


    def test_return_type_posting_data(self):
        self.assertIsInstance(self.iterated_df, pd.DataFrame)        


    def test_len_posting_data(self):
        self.assertEqual(self.iterated_df.shape[0], 3)


    def test_columns_posting_data(self):
        self.assertEqual(self.iterated_df.shape[1], 16)


if __name__ == '__main__':
    unittest.main()
