from sqlitedict import SqliteDict
from nltk.corpus import stopwords
from nltk import sent_tokenize, word_tokenize
import re
from collections import defaultdict
import json
from utils import *
import pickle

def process_ref(ref, title=None, sentence=None):
    title = ref["title"]
    url = ref["url"]
    content = re.sub(r"\n|\s", " ", ref["text"])
    if len(content) < 128: return None

    if not check_eng(content) & check_match(content, title):
        return None
    
    content = clean_text(content)
    content = passage_truncator(content)
    return [title, content, url]


def process_wiki_page(wiki):
    wiki = json.loads(wiki)
    rtn = []
    rtn.append(wiki["url"])
    rtn.append(wiki["title"])

    cleaned_wike_text = clean_text(wiki["text"])

    content = []
    for line in cleaned_wike_text.split("\n"):
        if not line: continue
        if line.startswith("<h2>"):
            content.append([line])
        else:
            content.append(custom_sent_tokenize(line))
    
    rtn.append(content)

    ref_ids = re.findall(r'\[[0-9]+\]', cleaned_wike_text)

    ref_to_id = defaultdict(list)
    refs = {}
    for ref in wiki["references"]:
        ref_id = ref["cite_id"]
        if ref_id not in ref_ids:
            continue
        
        for sub_ref in ref["sub_ref"]:
            refs[sub_ref["url"]] = [sub_ref["title"], sub_ref["text"]]
            ref_to_id[ref_id].append(sub_ref["url"])

    rtn.append(ref_to_id)
    return rtn, refs

if __name__ == "__main__":
    from tqdm import tqdm
    import os

    raw_file = "/data02/lilei/WebBrain/output_all_chapter/wikiRef_output_chapter_8"

    refs_db_path = "data/references.sqlite"
    wiki_db_path = "data/wiki.sqlite"
    url_db_path = "data/url_db.pkl"

    if os.path.exists(url_db_path):
        ref_url_db = pickle.load(open(url_db_path, "rb"))
    else:
        ref_url_db = set()

    refs_db = SqliteDict("data/references.sqlite")
    wiki_db = SqliteDict("data/wiki.sqlite")
    
    interval = 0
    for line in tqdm(open(raw_file)):
        passage, refs = process_wiki_page(line)
        wiki_db[passage[0]] = passage[1:]
        for url in refs:
            if url in ref_url_db:
                continue
            ref_url_db.add(url)
            refs_db[url] = refs[url]
        interval += 1
        if interval % 10000 == 0 and interval:
            refs_db.commit()
            wiki_db.commit()
    refs_db.commit()
    wiki_db.commit()
    with open("data/url_db.pkl", "wb") as f:
        pickle.dump(ref_url_db, f)

