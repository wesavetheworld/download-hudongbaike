#-*- coding: utf-8 -*-

"""This script downloads all articles under people category
on Hudong Baike.
"""

import html5lib
import HTMLParser
import urllib
import urllib2
import socket
import time
import sqlite3
from xml.etree.ElementTree import tostring


#Category page URL.
CATEGORY_URL = "http://fenlei.baike.com/%s"
#Word list page for a category.
WORD_LIST_URL = "http://fenlei.baike.com/%s/list"
#Wiki page URL.
WIKI_URL = "http://www.baike.com/wiki/%s"
#Timeout for connection.
DEFAULT_TIMEOUT = 60
#Root category.
ROOT_CATEGORY = u"页面总分类"
#Words that may cause connection reset.
BANNED_WORDS = [u"邓正来", u"盛雪", u"彭小枫", u"章沁生", u"王顺喜",
                u"王斌余", u"许传玺", u"荣高棠"]
#Final Database filename
DB_FILENAME = "hudongbaike.db"
#Progress Database filename
PROGRESS_DB_FILENAME = "progress.db"
#Retry time
RETRY_TIME = 30


def sql_escape(s):
    """Escape a string to SQL string value."""
    if s is None:
        return "null"
    return "'" + s.replace("'", "''") + "'"


def html_unescape(s):
    """Unescape html code."""
    h = HTMLParser.HTMLParser()
    return h.unescape(s)


def get_html(url):
    """Download html from a specific URL."""
    while True:
        try:
            response = urllib2.urlopen(url)
            return response.read()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print e
            if "reset" in str(e):
                raw_input("Connection reset.  Press ENTER to retry.")
            print "Retry in %d seconds." % RETRY_TIME
            time.sleep(RETRY_TIME)


def get_sub_categories(category):
    """Extract sub categories of a category."""
    #Download HTML.
    print (u"Downloading category: %s" % category).encode("utf-8")
    url = CATEGORY_URL % urllib.quote(category.encode("utf-8"))
    doc = html5lib.parse(get_html(url), namespaceHTMLElements=False)

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


def download_categories(db_conn):
    """Download all people category using BFS."""
    #Load progress from database.
    cursor = db_conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS Category (category)")
    cursor.execute("CREATE TABLE IF NOT EXISTS Frontier (category)")
    all_categories = set(row[0] for row in cursor.execute(
        "SELECT * FROM Category"))
    frontier = [row[0] for row in cursor.execute(
        "SELECT * FROM Frontier")]

    #If no category downloaded, starts with root category.
    if not all_categories:
        all_categories = set([ROOT_CATEGORY])
        frontier = [ROOT_CATEGORY]
        cursor.execute("INSERT INTO Category VALUES (%s)"
                       % sql_escape(ROOT_CATEGORY))
        cursor.execute("INSERT INTO Frontier VALUES (%s)"
                       % sql_escape(ROOT_CATEGORY))
    db_conn.commit()

    while frontier:
        #Expand frontier.
        c = frontier.pop(0)
        cursor.execute("DELETE FROM Frontier WHERE category = %s"
                       % sql_escape(c))
        new_categories = get_sub_categories(c)
        for category in new_categories:
            if category not in all_categories:
                frontier.append(category)
                all_categories.add(category)
                cursor.execute("INSERT INTO Frontier VALUES (%s)"
                               % sql_escape(category))
                cursor.execute("INSERT INTO Category VALUES (%s)"
                               % sql_escape(category))
        db_conn.commit()

    print "Found %d categories" % len(all_categories)
    return all_categories


def get_words(category):
    """Download all available words under a category."""
    #Download HTML.
    print (u"Downloading word list: %s" % category).encode("utf-8")
    url = WORD_LIST_URL % urllib.quote(category.encode("utf-8"))
    doc = html5lib.parse(get_html(url), namespaceHTMLElements=False)

    #Find all words.
    a_list = doc.findall(".//dd/a")
    words = set(a.text.strip() for a in a_list)
    print "Found %d words." % len(words)
    return words


def get_word_list(categories, db_conn):
    """Download all available words under a list of categories."""
    #Load progress from database.
    cursor = db_conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS Downloaded (category)")
    cursor.execute("CREATE TABLE IF NOT EXISTS Word (word)")
    downloaded = set(row[0] for row in cursor.execute(
        "SELECT * FROM Downloaded"))
    words = set(row[0] for row in cursor.execute("SELECT * FROM Word"))
    db_conn.commit()

    for c in categories:
        if c not in downloaded:
            new_words = get_words(c)
            for w in new_words:
                if w not in words:
                    cursor.execute("INSERT INTO Word VALUES (%s)"
                                   % sql_escape(w))
            words = words.union(new_words)
            cursor.execute("INSERT INTO Downloaded VALUES (%s)"
                           % sql_escape(c))
            db_conn.commit()

    print "Found %s words." % len(words)
    return words


def get_content(word):
    """Return page content for a word."""
    #Download HTML.
    print (u"Downloading page: %s" % word).encode("utf-8")
    url = WIKI_URL % urllib.quote(word.encode("utf-8"))
    doc = html5lib.parse(get_html(url), namespaceHTMLElements=False)

    try:
        #Parse HTML.
        summary_div = doc.find(".//div[@id='summary']")
        summary = ""
        if summary_div:
            summary = html_unescape(tostring(summary_div))
        content_div = doc.find(".//div[@id='content']")
        if not content_div:
            #Not found.
            print "Page not found!"
            return None
        content = html_unescape(tostring(content_div))
        return summary + content
    except UnicodeEncodeError:
        #Cannot decode.
        print "Unicode error!"
        return None


def download_words(words, db_filename=DB_FILENAME):
    """Download pages given a word list and save into a database file."""
    #Create database.
    db_conn = sqlite3.connect(db_filename)
    db_cursor = db_conn.cursor()
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Page (title, content)")

    #Retrieve progress
    downloaded = set(row[0] for row in db_cursor.execute(
        "SELECT DISTINCT title FROM Page"))
    print "Already have %s pages." % len(downloaded)

    try:
        print "Start downloading."
        for w in words:
            if w not in downloaded and w not in BANNED_WORDS:
                title = sql_escape(w)
                content = sql_escape(get_content(w))
                print "Saving to database."
                db_cursor.execute((u"INSERT INTO Page VALUES (%s, %s)"
                                   % (title, content)).encode("utf-8"))
                db_conn.commit()
    finally:
        db_conn.close()


def main():
    """Download all pages about people on Hudong Baike."""
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    progress_db_conn = sqlite3.connect(PROGRESS_DB_FILENAME)
    try:
        categories = download_categories(progress_db_conn)
        words = get_word_list(categories, progress_db_conn)
        download_words(words)
    finally:
        progress_db_conn.close()


if __name__ == "__main__":
    main()
