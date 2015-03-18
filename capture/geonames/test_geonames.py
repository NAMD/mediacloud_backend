import unittest
import os
from capture.geonames import update_geoindex
import pandas as pd
import datetime

class TestGeonames(unittest.TestCase):
    def test_download_table(self):
        fn  = update_geoindex.download_table(update_geoindex.DATA_URL)
        self.assertTrue(os.path.exists(fn))

    def test_load_table_from_disk(self):
        fn = update_geoindex.download_table(update_geoindex.DATA_URL)
        reader = update_geoindex.load_data_file(fn)
        self.assertIsInstance(reader, pd.io.parsers.TextFileReader)

    def test_chunk_is_dataframe(self):
        fn = update_geoindex.download_table(update_geoindex.DATA_URL)
        reader = update_geoindex.load_data_file(fn)
        df = reader.get_chunk(5)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 5)

    def test_dates_are_being_parsed(self):
        fn = update_geoindex.download_table(update_geoindex.DATA_URL)
        reader = update_geoindex.load_data_file(fn)
        df = reader.get_chunk(5)
        dates = sum([isinstance(i, datetime.date) for i in df.ix[0]])
        self.assertGreaterEqual(dates, 1, "{}: {}".format(i, df.ix[0]))

    def test_column_names_are_ok(self):
        columns = ["geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude", "feature class",
               "feature code", "country code", "cc2", "admin1 code", "admin2 code", "admin3 code", "admin4 code",
               "population", "elevation", "dem", "timezone", "modification date"]
        fn = update_geoindex.download_table(update_geoindex.DATA_URL)
        reader = update_geoindex.load_data_file(fn)
        df = reader.get_chunk(5)
        self.assertListEqual(list(df.columns), columns)


if __name__=="__main__":
    unittest.main()