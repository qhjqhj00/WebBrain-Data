import re
from nltk import sent_tokenize, word_tokenize
from itertools import chain

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
    
def clean_text(text):
    text = re.sub(r"(<a href=\"http(s)*\S+\">)|(</a>)", "", text) 
    text = re.sub(r"Coordinates :(.+)\n", "", text) 
    text = re.sub("You can help Wikipedia by expanding it .", "", text)
    text = re.sub(r"&#(\d+);|<0x(.{1,3})>|\\xa0|&amp", " ", text)
    text = re.sub(r"(\.\S+)-(\S+)", " ", text)
    text = re.sub(r".longitude|.latitude", "", text)
    text = re.sub(r"{\S+}", " ", text)
    text = re.sub(r"\(\S+\)", " ", text)
    text = re.sub(r"may refer to:|You can help Wikipedia by expanding it .|\[citation needed\]", " ", text)
    text = re.sub(r"url\(.+\)", " ", text)
    text = re.sub(r" +", " ", text)
    return text

def custom_sent_tokenize(text):
    ori_sentences = sent_tokenize(text)
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
    if current_sent:
        sentences.append(current_sent)
    return sentences

def passage_truncator(text, max_length=256):
    rtn = []
    tmp = []
    length = 0
    last_sent = ""
    for sent in sent_tokenize(text):
        sent_length = len(sent.split())
        if sent_length > 256: continue # ignore longer sentence
        if length + sent_length > max_length:
            rtn.append(" ".join(tmp))
            if len(last_sent.split()) < max_length:
                tmp = []
            length = 0
        length += sent_length
        tmp.append(sent)
        last_sent = sent
    if tmp:
        rtn.append(" ".join(tmp))
    # rtn = [p for p in rtn if len(p.split()) > 100] # TODO
    return rtn

def passage2sent(passage):
    refs = passage[-1]
    rtn = []
    
    for section in passage[1]:
        if len(section) <= 1:
            continue
        ref_buffer = []
        for i in range(len(section)-1, -1, -1):
            sent = section[i]
            if re.match("\[[0-9]+\]", sent):
                if sent not in refs:
                    continue
                ref_buffer.append(sent)
            elif ref_buffer:
                rtn.append([ref_buffer, sent])
                ref_buffer = []
            else:
                continue
    return rtn, refs
