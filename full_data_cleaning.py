import os
import json
import re
from typing import List, Dict
from itertools import chain
from collections import defaultdict
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from pyspark import SparkContext, SparkConf,SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.types import (StringType, ArrayType, DoubleType,
                    StructField, StructType, IntegerType)
from pyspark.sql import functions as F


stops = set(stopwords.words('english')).union(set(",.{}[]-+:;!?/`~''\"()<>@#$%^&*|\\"))

## 1. clean and separate data
def process_wiki_raw(line):
    wiki = json.loads(line)
    url = wiki.get("url")
    title = wiki.get("title")
    text = wiki.get("text")
    references = wiki.get("references")
    ret = []
    text = clean_text(text)
    sections, headings= find_h2(text)

    for i, section in enumerate(sections):
        prefix = title+"[sep]"
        cur_query = prefix+"introduction" if i == 0 else prefix + headings[i-1]
        ref_ids = re.findall(r'\[[0-9]+\]', section)
        cur_refs = []
        for ref in references:
            ref_id = ref["cite_id"]
            if ref_id not in ref_ids:
                continue
            for sub_ref in ref["sub_ref"]:
                processed_ref = process_ref(sub_ref)
                if processed_ref is None:
                    continue
                else:
                    ref_title, ref_content, ref_url = processed_ref
                    cur_refs.append([ref_id, ref_url, ref_title, ref_content])
        ret.append([url, cur_query, section, cur_refs])

    return ret


def clean_text(text):
    text = re.sub(r"(<a href=\"http(s)*\S+\">)|(</a>)", "", text)
    text = re.sub(r"&amp", "", text)
    text = re.sub(r"Coordinates :(.+)\n", "", text) 
    text = re.sub("You can help Wikipedia by expanding it .", "", text)
    text = re.sub(r"&#(\d+);|<0x(.{1,3})>|\\xa0", " ", text)
    text = re.sub(r"(\.\S+)-(\S+)", " ", text)
    text = re.sub(r".longitude|.latitude", "", text)
    text = re.sub(r"{\S+}", " ", text)
    text = re.sub(r"\(\S+\)", " ", text)
    text = re.sub(r"may refer to:|You can help Wikipedia by expanding it .", " ", text)
    text = re.sub(r"\[citation needed\]|\[edit\]|\[ edit\ ]", " ", text)
    text = re.sub(r"url\(.+\)", " ", text)
    text = re.sub(r" +", " ", text)
    text = re.sub(r"<h3>|</h3>>", "", text)
    text = re.sub(r"<h4>|</h3>", "", text)
    return text


def find_h2(text):
    spans = [obj for obj in re.finditer("<h2>", text)]
    spans_end = [obj for obj in re.finditer("</h2>", text)]
    all_sections = []  # n+1
    all_headings = []  # n
    if len(spans) != len(spans_end):  # if not matching, return lead section
        potential_end_idx = len(text)
        if spans:
            potential_end_idx = min(potential_end_idx, spans[0].start())
        if spans_end:
            potential_end_idx = min(potential_end_idx, spans_end[0].start())
        return [text[:potential_end_idx]], []

    last_section_start_idx = 0
    for i in range(len(spans)):
        left_tag_start_idx, left_tag_end_idx = spans[i].start(), spans[i].end()
        right_tag_start_idx, right_tag_end_idx = spans_end[i].start(), spans_end[i].end()
        last_section = text[last_section_start_idx:left_tag_start_idx] # get last section
        last_section_start_idx = right_tag_end_idx + 1
        all_sections.append(last_section)
        if len(last_section) < 10 and i != 0: # if last section is too short, skip last section
            all_headings = all_headings[:-1]
            all_sections = all_sections[:-1]
        cur_heading = text[left_tag_end_idx:right_tag_start_idx]
        if "\n" in cur_heading or len(cur_heading.split(" "))>10: # return all sections before current heading
            return all_headings, all_sections
        all_headings.append(cur_heading.lower().strip())
    last_section=text[last_section_start_idx:]
    all_sections.append(last_section)

    if len(all_sections) != 1 and len(last_section) < 10: # if last section is too short, skip last section
            all_headings = all_headings[:-1]
            all_sections = all_sections[:-1]
    if len(all_headings) >= 10: # if too many headings, return only lead section
        return all_sections[:1], []
    assert len(all_sections) == len(all_headings) + 1, text
    return all_sections, all_headings


def process_ref(ref, title=None):
    title = ref["title"]
    url = ref["url"]
    content = re.sub(r"\n|\s", " ", ref["text"])
    if len(content) < 128: 
        return None
    if not check_eng(content) & check_match(content, title):
        return None
    content = clean_text(content)
    return title, content, url


def check_eng(text):
    non_eng = re.findall(
        r'[^a-z_A-Z\d\.!@#\$%\\\^&\*\)\(\+=\{\}\[\]\/",\'<>~\·`\?:; |]', 
        text)
    if len(non_eng) / len(text) > 0.3:
        return False
    else:
        return True


def check_match(text, target):
    text = set(text.lower().split())
    target = set(target.lower().split())
    if target is None:
        return True
    elif len(text & target) > 0:
        return True
    else:
        return False


## 2. generate train data
"""
    input:
        text = str
    output:
        coarse_map = [ [sent, [id, ...]], ...]
"""
def coarse_ref_map(text):
    sentences = custom_sent_tokenize(text.lower())
    rtn = []
    if len(sentences) == 1:
        return [[sentences[0], ["[0]"]]]
    for i in range(len(sentences)-1):
        if re.match("\[[0-9]+\]", sentences[i+1]):
            if not re.match("\[[0-9]+\]", sentences[i]):
                rtn.append([sentences[i], [sentences[i+1]]])
            else:
                try:
                    rtn[-1][1].append(sentences[i+1])
                except:
                    continue
        elif not re.match("\[[0-9]+\]", sentences[i]):
           rtn.append([sentences[i], ["[0]"]])
        if i == len(sentences)-2 and not re.match("\[[0-9]+\]", sentences[i+1]):
            rtn.append([sentences[i+1], ["[0]"]])
    return rtn


"""
    input:
        text = str
    output:
        coarse_map = [sent, ...]
"""
def custom_sent_tokenize(text):
    ori_sentences = sent_tokenize(text.replace("\n", ""))
    ori_sentences = list(
            chain(*[re.split("(\[[0-9]+\])", sent)    
             for sent in ori_sentences
            ]))
    ori_sentences = [sent for sent in ori_sentences if len(sent.strip()) != 0]
    sentences = []

    current_sent = ""
    for sent in ori_sentences:
        if re.match("\[[0-9]+\]", sent):
            if current_sent != "":
                sentences.append(current_sent)
                current_sent = ""
            sentences.append(sent)
        elif sent.endswith(("e.g.","i.e.")) or len(sent) < 8:
            current_sent += f" {sent}"
        elif current_sent.endswith((".", "!", "?", ".\"", "?\"", "!\"")):
            current_sent += f" {sent}"
            sentences.append(current_sent)
            current_sent = ""
        else:
            current_sent += f" {sent}"
    return sentences


"""input:
        coarse_map = [ [sent, [id, ...]], ...]
        refs = [ [id,url,title,text],...]
    output:
        coarse_map = [ [sent, [id, ...]], ...]
        rtn_passage = {id:[str, title], ...}
"""
def refine_ref_map(coarse_map, refs):
    ref_ids = set(list(chain(*[p[1] for p in coarse_map])))
    if ref_ids == set(["[0]"]) or len(refs) == 0:
        return coarse_map, {}
    ref_passage = defaultdict(list)
    for ref in refs:
        ref_id = ref[0]
        ref_title = ref[2]
        if ref_id not in ref_ids or len(ref[-1].strip()) == 0:
            continue
        ref_passage[ref_id].extend(split_text(f"{ref[3]}", 256))
        ref_passage[ref_id].append(ref_title)  # title line: 230+139+260+293
    
    current_ref_ids = []
    rtn_passage = {}
    trace_back_count = 0

    for i in range(len(coarse_map)-1, -1, -1): 
        if coarse_map[i][1] == ["[0]"] and not current_ref_ids:
            continue
        elif coarse_map[i][1] != ["[0]"]:
             current_ref_ids = coarse_map[i][1]
             trace_back_count = 0
        else:
            if trace_back_count > 3:
                continue
            check_ref = check_cited(coarse_map[i][0], ref_passage, current_ref_ids)
            if check_ref:
                coarse_map[i][1] = check_ref
                trace_back_count += 1
            else:
                trace_back_count += 3
    rtn_passage = rank_ref_passage(ref_passage, coarse_map)
    return coarse_map, rtn_passage


"""
    input:
        text = str
        maxlem = int
    output:
        sub_texts = [[w1, w2 ...], [w1, w2 ...] ...]
"""
def split_text(text, maxlen, greedy=False):
    sentences = sent_tokenize(text.lower())
    sentences = [word_tokenize(sent) for sent in sentences]
    sent_lens = [len(s) for s in sentences]
    if sum(sent_lens) <= maxlen:
        return [list(chain(*sentences))]
    n_sentences = len(sentences)
    alls = []  
    for i in range(n_sentences):
        length = 0
        sub = []
        for j in range(i, n_sentences):
            if length + sent_lens[j] <= maxlen or not sub:
                sub.append(j)
                length += sent_lens[j]
            else:
                break
        alls.append(sub)
        if j == n_sentences - 1:
            if sub[-1] != j:
                alls.append(sub[1:] + [j])
            break

    if len(alls) == 1:
        return [list(chain(*sentences))]
    if greedy:  
        sub_texts = [list(chain(*[sentences[i] for i in sub])) for sub in alls]
        return sub_texts
    else:  # 用动态规划求解满足要求的最优子片段集
        DG = {}
        N = len(alls)
        for k in range(N):
            tmplist = list(range(k + 1, min(alls[k][-1] + 1, N)))
            if not tmplist:
                tmplist.append(k + 1)
            DG[k] = tmplist
        routes = {}
        routes[N] = (0, -1)
        for i in range(N - 1, -1, -1):
            templist = []
            for j in DG[i]:
                cross = set(alls[i]) & (set(alls[j]) if j < len(alls) else set())
                w_ij = sum([sent_lens[k] for k in cross]) ** 2  # 第i个节点与第j个节点交叉度
                w_j = routes[j][0]  # 第j个子问题的值
                w_i_ = w_ij + w_j
                templist.append((w_i_, j))
            routes[i] = min(templist)
        sub_texts = [list(chain(*[sentences[i] for i in alls[0]]))]
        k = 0
        while True:
            k = routes[k][1]
            sub_texts.append(list(chain(*[sentences[i] for i in alls[k]])))
            if k == N - 1:
                break
    return sub_texts


"""
    input:
        src = str
        refs = {id: [[w1, w2 ...], [w1, w2 ...], ..., title], ...}
        ref_ids = [id,...]
    output:
        rtn_ids [id,...]
"""
def check_cited(src, refs, ref_ids):
    rtn_ids = []
    src_toks = set(word_tokenize(src)) - stops
    if len(src_toks) == 0:
        return rtn_ids
    for ref_id in ref_ids:
        ref_toks = refs[ref_id]
        ref_toks = set(list(chain(*ref_toks))) - stops
        if len(ref_toks & src_toks)/ len(src_toks) > 0.3:
            rtn_ids.append(ref_id)
    return rtn_ids


"""
    input:
        ref_passage = {id: [[w1, w2 ...], [w1, w2 ...] ...title], ...}
        refs_map = [ [sent, [id, ...]], ...]
    output:
        rtn = {id:[str, title], ...}
"""
def rank_ref_passage(ref_passage, ref_map):
    rtn = {}
    src_toks_by_ref = defaultdict(list)
    for sent in ref_map:
        for ref_id in sent[1]:
            src_toks_by_ref[ref_id].extend(word_tokenize(sent[0]))
    for ref_id in ref_passage:
        tok_count = []
        for p in ref_passage[ref_id][:-1]:
            n = compute_match(p, src_toks_by_ref[ref_id])
            tok_count.append(n)
        if len(tok_count) == 0:
            continue
        passage_idx = tok_count.index(max(tok_count))
        rtn[ref_id] = [" ".join(ref_passage[ref_id][:-1][passage_idx]), ref_passage[ref_id][-1]]

    return rtn

"""
    input:
        src = [w1, w2 ...]
        trg = [w1, w2 ...]
    output:
        rtn = int
"""
def compute_match(src, trg) -> int:
    src_toks = set(src) - stops
    trg_toks = set(trg) - stops
    return len(src_toks & trg_toks)


## 3. after process

"""
    input:
        coarse_map = [ [sent, [id, ...]], ...]
        rtn_passage = {id:[str,title], ...}
    output:
        text = [[sent, id, ...], ...]
        refs = [[id, title, str], ... ]
"""
def after_process(coarse_map, rtn_passage):
    text, refs = [], []
    for sent in coarse_map:
        text.append([sent[0]])
        text[-1].extend(sent[1])
    for k,v in rtn_passage.items():
        refs.append([k,v[1],v[0]])
    refs.sort(key=lambda x:[x[0]])

    return text, refs


# main
def main():
    spark = SparkSession.builder \
            .appName("data_cleaning") \
            .enableHiveSupport() \
            .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set("spark.sql.broadcastTimeout", 1000)
    sc = spark.sparkContext

    hdfs_path = "/user/webbrain/output_all_chapter/*" 

    num_partition = 100000
    data = sc.textFile(hdfs_path, minPartitions=num_partition)
    schema = StructType([
                    StructField("url", StringType())
                    ,StructField("title", StringType())
                    ,StructField("text", ArrayType(ArrayType(StringType())))
                    ,StructField("references", ArrayType(ArrayType(StringType())))
                    ])

    # process raw file
    data = data.flatMap(lambda x: process_wiki_raw(x))
    # save middle part

    # generate train data
    # x = [url, title, text, refs]
    data = data.map(lambda x: (x[0], x[1], coarse_ref_map(x[2]), x[3]))\
                .map(lambda x: (x[0], x[1], refine_ref_map(x[2], x[3])))\
                .map(lambda x: (x[0],x[1], after_process(x[2][0], x[2][1])))\
                .map(lambda x: (x[0],x[1],x[2][0],x[2][1]))\
                .toDF(schema)
                
    # save
    cur_save_path = "/user/webbrain/result_full"  # 运行前一定不能有这个dir不然会报错
    data.write.format('json').save(cur_save_path)


if __name__ == '__main__':
    main()