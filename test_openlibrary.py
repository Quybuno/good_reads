"""Integration tests for the OpenLibrary public API.

These tests make real HTTP requests to https://openlibrary.org and require
network access.

Run all tests:
    python3 -m unittest -v test_openlibrary

Run a single test:
    python3 -m unittest test_openlibrary.OpenLibraryBooksAPITest.test_fetch_book_by_isbn
"""

import unittest

import requests

BOOKS_API = "https://openlibrary.org/api/books"
SEARCH_API = "https://openlibrary.org/search.json"

# ISBN:9780140328721 -> "Fantastic Mr. Fox" by Roald Dahl (OL7353617M)
BOOK_ISBN = "ISBN:9780140328721"


class OpenLibraryBooksAPITest(unittest.TestCase):
    """Tests for GET https://openlibrary.org/api/books."""

    def test_fetch_book_by_isbn(self):
        response = requests.get(
            BOOKS_API,
            params={
                "bibkeys": BOOK_ISBN,
                "format": "json",
                "jscmd": "data",
            },
            timeout=10,
        )

        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn(BOOK_ISBN, data)

        book = data[BOOK_ISBN]
        self.assertEqual(book["title"], "Fantastic Mr. Fox")
        self.assertIn("authors", book)
        self.assertTrue(book["authors"])

    def test_missing_bibkeys_returns_422(self):
        response = requests.get(
            BOOKS_API,
            params={"format": "json"},
            timeout=10,
        )

        self.assertEqual(response.status_code, 422)
        self.assertIn("detail", response.json())


class OpenLibrarySearchAPITest(unittest.TestCase):
    """Tests for GET https://openlibrary.org/search.json."""

    def test_search_returns_documents(self):
        response = requests.get(
            SEARCH_API,
            params={"q": "the hobbit", "limit": 5},
            timeout=10,
        )

        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertGreater(data["numFound"], 0)
        self.assertTrue(data["docs"])
        self.assertTrue(all("title" in doc for doc in data["docs"]))


if __name__ == "__main__":
    unittest.main()
