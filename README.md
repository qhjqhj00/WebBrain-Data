# WebBrain: Learning to Generate Factually Correct Articles for Queries by Grounding on Large Web Corpus



## Introduction
In this paper, we introduce a new NLP task -- generating short factual articles with references for queries by mining supporting evidence from the Web. In this task, called WebBrain, the ultimate goal is to generate a fluent, informative, and factually-correct short article (e.g., a Wikipedia article) for a factual query unseen in Wikipedia. To enable experiments on WebBrain, we construct a large-scale dataset WebBrain-Raw by extracting English Wikipedia articles and their crawlable Wikipedia references. WebBrain-Raw is ten times larger than the previous biggest peer dataset, which can greatly benefit the research community. From WebBrain-Raw, we construct two task-specific datasets: WebBrain-R and WebBrain-G, which are used to train in-domain retriever and generator, respectively. Besides, we empirically analyze the performances of the current state-of-the-art NLP techniques on WebBrain and introduce a new framework ReGen, which enhances the generation factualness by improved evidence retrieval and task-specific pre-training for generation. Experiment results show that ReGen outperforms all baselines in both automatic and human evaluations.

## Data Files

### Application form

You're required to sign the application form and send in to us before you download the datasets. 

[Application Form](https://github.com/qhjqhj00/WebBrain-Data/blob/main/application_form.pdf)

Contact mail: ian[at]ruc.edu.cn

**We provide the following datasets:**
### WebBrain-Raw
This dataset contains the raw text of WebBrain. It comprises 153 zipped data chunks in which each line is a Wikepedia page with its reference articles. The statistic information of WebBrain-Raw is as:


| Dataset     | \# Wiki Pages | \# Refs   | Status        | Storage Size |
| ----------- | ------------- | ---------| ------------ | ------------ |
| WikiSum (Liu et al., 2018)      | 2.3M          | 87M      | Need crawling | 300GB        |
| WikiCatSum (Perez-Beltrachini et al., 2019) | 0.17M  | 23.5M    | Ready        | 4.8GB        |
| Hiersumm (Liu & Lapata, 2019)   | 1.66M | -        | Ready        | 6.9GB        |
| WebBrain-Raw | 14.86M        | 259.5M   | Ready        | 2.8TB        | 


Download Link: [Link](https://github.com/qhjqhj00/WebBrain-Data/)

Baidu Disk: [Link](https://github.com/qhjqhj00/WebBrain-Data/)


### WebBrain-deduplicated
In WebBrain-Raw, multiple Wikipedia pages might use an identical web page as a reference, leading to much redundancy. In this dataset, we deduplicate all reference articles and generate a standalone reference database. We only keep the reference's URL in the Wikipedia page data.

Download Link: [Link](https://github.com/qhjqhj00/WebBrain-Data/)

Baidu Disk: [Link](https://github.com/qhjqhj00/WebBrain-Data/)


### WebBrain-G(eneration)

This is a processed dataset for training and evaluating generation model. 

WebBrain-G contains train / dev / test files, which are in the following format:

```
[title] wiki_title [ref] [ref_id] ref_title ref_content [SPLIT] ... [SPLIT] target_text 
```
where we append the Wiki title to the front of each reference, merge all references and the target text (label) with a special token [SPLIT].

Download Link: [Link](https://github.com/qhjqhj00/WebBrain-Data/)

Baidu Disk: [Link](https://github.com/qhjqhj00/WebBrain-Data/)

### WebBrain-R(etrieval)

This is a processed dataset for training and evaluating retrieval model.

WebBrain-R contains four files: train.tsv / dev.tsv / test.tsv and corpus.jsonl. 
The first three files are in the same format:
```
qid\tquery\tpositive_passage_id\tnegative_passage1_id\t...\n
```
And data in corpus.jsonl are in the fowllowing format:
```
{"id": "passage_id", "content": "passage_content"}
```


Download Link: [Link](https://github.com/qhjqhj00/WebBrain-Data/)

Baidu Disk: [Link](https://github.com/qhjqhj00/WebBrain-Data/)

The statistic information of WebBrain-R and WebBrain-G is as follow:
#### Statistics of data for experiments.

|                       | WebBrain-R | WebBrain-G |
|-----------------------|------------|------------|
| \# Queries            | 2.74M      | 12.32M     |
| \# Ref. passages      | 3.20M      | 12.61M     |
| \# Tokens / Query     | 3.2        | 2.9        |
| \# Tokens / Passage   | 237.5      | 250.0      |
| \# Tokens / Target    | -          | 108.6      |
| \# Training           | 4.46M      | 12.30M     |
| \# Validation         | 0.2M       | 0.5M       |
| \# Test               | 88,935     | 24,546     |

In the paper, we evaluate a proposed model, ReGen on the WebBrain dataset. We release the source codes of ReGen in this Repo: [Link](https://github.com/qhjqhj00/WebBrain).

# Terms of Use

- The dataset is provided "as is" and without warranty or support of any kind.
- You may use the dataset for research and educational purposes only.
- You may not use the dataset for any commercial purpose, including without limitation any resale, license, or other exploitation of the data.
- You may not distribute or share the dataset with others without the prior written consent of the dataset owner.
- You must acknowledge the dataset owner and source (WebBrain) in any publication or presentation that makes use of the data.
- You have the right to request removal of your data from the dataset. The dataset owner will make reasonable efforts to remove your data within a reasonable time period, subject to technical limitations and legal requirements.
- The dataset owner may terminate your access to the dataset at any time for any reason.

# FAQ

# Citation


