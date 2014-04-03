# coding: utf-8

import datetime
import unittest

from utilities.parsedates import parse_pt_date


class TestParsePtDate(unittest.TestCase):
    def test_regular_date(self):
        result = parse_pt_date('Seg, 21 Out 2013 21:14:36 -0200')
        expected = datetime.datetime(2013, 10, 21, 23, 14, 36)
        self.assertEqual(result, expected)

    def test_date_in_full(self):
        result = parse_pt_date(u'terça-feira, 9 de outubro de 2012')
        expected = datetime.datetime(2012, 10, 9, 0, 0, 0)
        self.assertEqual(result, expected)

    def test_gmt_timezone(self):
        result = parse_pt_date(u'Ter, 31 Jul 2012 16:54:00 GMT')
        expected = datetime.datetime(2012, 7, 31, 16, 54, 0)
        self.assertEqual(result, expected)

    def test_compact_date(self):
        result = parse_pt_date('23 Abr 2008')
        expected = datetime.datetime(2008, 4, 23, 0, 0, 0)
        self.assertEqual(result, expected)

    def test_compact_date_month_in_full(self):
        result = parse_pt_date('23 de Dezembro de 2013')
        expected = datetime.datetime(2013, 12, 23, 0, 0, 0)
        self.assertEqual(result, expected)

    def test_date_with_error_before(self):
        date_string = u":  Use of undefined constant dataini_con - assumed 'dataini_con' in  on line \n\n:  A non well formed numeric value encountered in  on line \nWed, 31 Dec 1969 20:33:33 -0300"
        result = parse_pt_date(date_string)
        expected = datetime.datetime(1969, 12, 31, 23, 33, 33)
        self.assertEqual(result, expected)
