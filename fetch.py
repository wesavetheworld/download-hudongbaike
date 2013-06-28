#-*- coding: utf-8 -*-

"""This script downloads all articles under people category
on Hudong Baike.
"""

import html5lib
import HTMLParser
import urllib
import urllib2
import os
import pickle
import sqlite3
from xml.etree.ElementTree import tostring


#Category page URL.
CATEGORY_URL = "http://fenlei.baike.com/%s"
#Word list page for a category.
WORD_LIST_URL = "http://fenlei.baike.com/%s/list"
#Wiki page URL.
WIKI_URL = "http://www.baike.com/wiki/%s"
#Words that may cause connection reset.
BANNED_WORDS = [u"邓正来", u"盛雪", u"彭小枫", u"章沁生", u"王顺喜",
                u"王斌余"]


def sql_escape(s):
    """Escape a string to SQL string value."""
    if s is None:
        return "null"
    return "'" + s.replace("'", "''") + "'"


def html_unescape(s):
    """Unescape html code."""
    h = HTMLParser.HTMLParser()
    return h.unescape(s)


def get_sub_categories(category):
    """Extract sub categories of a category."""
    #Download HTML.
    print (u"Downloading category: %s" % category).encode("utf-8")
    url = CATEGORY_URL % urllib.quote(category.encode("utf-8"))
    doc = html5lib.parse(urllib2.urlopen(url).read(),
                         namespaceHTMLElements=False)

    #Find all words.
    categories = set()
    headers = doc.findall(".//div[@class='sort']/h3")
    paragraphs = doc.findall(".//div[@class='sort']/p")
    for h, p in zip(headers, paragraphs):
        if h.text.strip() == u"下一级分类专题":
            links = p.findall("a")
            for link in links:
                categories.add(link.text.strip())
    return categories


def download_categories(pickle_filename="category.dat"):
    """Download all people category using BFS."""
    #Starts with root category.
    all_categories, frontier = set([u"页面总分类"]), [u"页面总分类"]

    #Load progress from pickle file.
    if os.path.exists(pickle_filename):
        with open(pickle_filename, "rb") as pickle_file:
            all_categories, frontier = pickle.load(pickle_file)

    while frontier:
        #Expand frontier.
        new_categories = get_sub_categories(frontier.pop(0))
        for category in new_categories:
            if category not in all_categories:
                frontier.append(category)
                all_categories.add(category)

        #Save progress in case of exception.
        print "Saving..."
        with open(pickle_filename, "wb") as pickle_file:
            pickle.dump((all_categories, frontier), pickle_file)

    print "Found %d categories" % len(all_categories)
    return all_categories


def get_words(category):
    """Download all available words under a category."""
    #Download HTML.
    print (u"Downloading word list: %s" % category).encode("utf-8")
    url = WORD_LIST_URL % urllib.quote(category.encode("utf-8"))
    doc = html5lib.parse(urllib2.urlopen(url).read(),
                         namespaceHTMLElements=False)

    #Find all words.
    words = set()
    a_list = doc.findall(".//dd/a")
    for a in a_list:
        words.add(a.text.strip())
    print "Find %d words." % len(words)
    return words


def get_word_list(categories, pickle_filename="words.dat"):
    """Download all available words under a list of categories."""
    downloaded, words = set(), set()
    #Load progress from pickle file.
    if os.path.exists(pickle_filename):
        with open(pickle_filename, "rb") as pickle_file:
            downloaded, words = pickle.load(pickle_file)

    counter = 0
    for c in categories:
        if c not in downloaded:
            words = words.union(get_words(c))
            downloaded.add(c)
            counter += 1
            #Save progress in case of exception.
            if counter % 5 == 0:
                print "Saving..."
                with open(pickle_filename, "wb") as pickle_file:
                    pickle.dump((downloaded, words), pickle_file)

    with open(pickle_filename, "wb") as pickle_file:
        pickle.dump((downloaded, words), pickle_file)

    print "Found %s words." % len(words)
    return words


def get_content(word):
    """Return page content for a word."""
    #Download HTML.
    print (u"Downloading page: %s" % word).encode("utf-8")
    url = WIKI_URL % urllib.quote(word.encode("utf-8"))
    doc = html5lib.parse(urllib2.urlopen(url).read(),
                         namespaceHTMLElements=False)

    #Parse HTML.
    content_div = doc.find(".//div[@id='content']")
    if not content_div:
        #Not found.
        print "Page not found!"
        return None
    try:
        return html_unescape(tostring(content_div))
    except UnicodeEncodeError:
        #Cannot decode.
        print "Unicode error!"
        return None


def download_words(words, db_filename="hudongbaike.db"):
    """Download pages given a word list and save into a database file."""
    #Create database.
    db_conn = sqlite3.connect(db_filename)
    db_cursor = db_conn.cursor()
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Page (title, content)")

    #Retrieve progress
    downloaded = set()
    for row in db_cursor.execute("SELECT title FROM Page"):
        downloaded.add(row[0])
    print "Already have %s pages." % len(downloaded)

    try:
        for w in words:
            if w not in downloaded and w not in BANNED_WORDS:
                title = sql_escape(w)
                content = sql_escape(get_content(w))
                print "Saving to database."
                db_cursor.execute((u"INSERT INTO Page VALUES (%s, %s)"
                                   % (title, content)).encode("utf-8"))
    finally:
        db_conn.commit()
        db_conn.close()


def main():
    """Download all pages about people on Hudong Baike."""
    categories = download_categories()
    words = get_word_list(categories)
    download_words(words)


if __name__ == "__main__":
    main()
