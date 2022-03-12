import unittest
from crawl import normalizeurl

class CrawlerTest(unittest.TestCase):
    """
    Test crawl functions.
    """

    def test_normalizeurl_simple(self):
        """Normalize simple url without params"""
        normalized = normalizeurl('https://google.com/')
        self.assertEqual(normalized, 'google.com')

    def test_normalizeurl_queryparams(self):
        """normalize a complex url with query params"""
        normalized = normalizeurl('https://abs.google.co.uk/index///?a=4&b=%20D')
        self.assertEqual('abs.google.co.uk/index?a=4&b=%20D', normalized)
