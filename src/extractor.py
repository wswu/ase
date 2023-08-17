import json
import os
import time
from argparse import ArgumentParser
from collections import namedtuple

import newspaper
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def roundup_url(i):
    return f"https://www.allsides.com/headline-roundups?page={i}"


def download_roundups(roundup_dir, delay=0.1):
    os.makedirs(roundup_dir, exist_ok=True)

    # download first roundup
    r = requests.post(roundup_url(0))
    with open(f"{roundup_dir}/0", "w") as fout:
        print(r.text, file=fout)

    # find the last page index
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a"):
        if a.get("title") == "Go to last page":
            last_page = int(a.get("href").split("=")[1])

    # download the rest of the roundups
    for i in tqdm(range(1, last_page + 1)):
        r = requests.post(roundup_url(i))
        with open(f"{roundup_dir}/{i}", "w") as fout:
            print(r.text, file=fout)
        time.sleep(delay)


def parse_roundup(path):
    roundups = []
    with open(path) as fin:
        soup = BeautifulSoup(fin.read(), "html.parser")

        for tr in soup.find_all("tr"):
            children = tr.find_all("td")

            # skip table header
            if len(children) == 0:
                continue

            title = children[0].find("a").text
            url = children[0].find("a").get("href")

            # topic and date may be empty
            topic = ""
            if (a := children[1].find("a")) is not None:
                topic = a.text
            date = ""
            if (span := children[2].find("span")) is not None:
                date = span.text

            roundups.append((title, url, topic, date))
    return roundups


def parse_roundups(roundup_dir):
    roundups = []
    for fn in os.listdir(roundup_dir):
        for roundup in parse_roundup(f"{roundup_dir}/{fn}"):
            roundups.append(roundup)
    # remove roundups with no date
    return sorted([r for r in roundups if r[3] != ""], key=lambda x: x[3])


Roundup = namedtuple("Roundup", ["title", "url", "topic", "date"])
Story = namedtuple("Story", ["title", "tags", "summary", "articles"])
Article = namedtuple("Article", ["title", "side", "source", "url"])


def read_roundups(path):
    roundups = []
    with open(path) as fin:
        for line in fin:
            roundups.append(Roundup(*line.strip().split('\t')))
    return roundups


def download_stories(roundups, story_dir, redownload=False, delay=0.1):
    os.makedirs(story_dir, exist_ok=True)

    for r in tqdm(roundups):
        # url format is "/story/long-story-name"
        dir = f"{story_dir}/{r.date}.{r.url.split('/')[2]}"
        os.makedirs(dir, exist_ok=True)

        story_file = f"{dir}/story.html"
        if redownload or not os.path.exists(story_file):
            req = requests.get(f"https://www.allsides.com{r.url}")
            with open(story_file, "w") as fout:
                print(req.text, file=fout)
            time.sleep(delay)


def parse_story_html(path):
    with open(path) as fin:
        soup = BeautifulSoup(fin.read(), "html.parser")

    story_title = soup.find("h1").text.strip()

    tags = []
    for div in soup.find_all("div", class_="page-tags"):
        for a in div.find_all("a"):
            tags.append(a.get("href").split("/")[-1])

    summary = soup.find("div", class_="story-id-page-description").text.strip()

    articles = []

    coverage = soup.find("div", class_="featured-coverage")

    if coverage is None:
        # 2015-07-28.iran-nuclear-deal-hearing/story.html and several others have a blank roundup page
        return None

    for div in coverage.find_all("div", class_="news-item"):
        title = div.find("a", class_="news-title").text.strip()

        try:
            # note that side may change over time (e.g. CNN from Left -> Lean Left)
            side = div.find("img").get("alt").split(": ")[1]
        except:
            # https://www.allsides.com/story/aca-means-millions-can-quit-or-cut-hours doesn't have a side
            # we can't normally use this for the side bc it doesn't have Lean Left and Lean Right
            side = div.get("class")[-1].capitalize()

        source = div.find("div", class_="news-source").text.strip()
        url = div.find("a", class_="external-link").get("href").strip()

        articles.append(Article(title, side, source, url))

    return Story(story_title, tags, summary, articles)


def parse_all_stories(story_dir):
    for dir in os.listdir(story_dir):
        if os.path.exists(f"{story_dir}/{dir}/story.json"):
            continue

        story = parse_story_html(f"{story_dir}/{dir}/story.html")

        if story is None:
            print("skipping", dir)
            continue

        with open(f"{story_dir}/{dir}/story.json", "w") as fout:
            # convert nested namedtuple to dictionary
            j = Story(story.title, story.tags, story.summary,
                      [a._asdict() for a in story.articles])._asdict()
            json.dump(j, fout, ensure_ascii=False)


def download_articles(story_dir, redownload=False, retry_errors=False, delay=0.1):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/116.0"}
    for dir in tqdm(os.listdir(story_dir)):
        if not os.path.exists(f"{story_dir}/{dir}/story.json"):
            continue
        with open(f"{story_dir}/{dir}/story.json") as fin:
            j = json.load(fin)
        for idx, article in enumerate(j["articles"]):
            html_file = f"{story_dir}/{dir}/{idx}.html"
            err_file = f"{story_dir}/{dir}/{idx}.err"
            if redownload \
                    or (retry_errors and os.path.exists(err_file)) \
                    or (not os.path.exists(html_file) and not os.path.exists(err_file)):
                try:
                    r = requests.get(article["url"], headers=headers)
                    if r.status_code == 200:
                        with open(html_file, "w") as fout:
                            print(r.text, file=fout)
                    else:
                        print("status code", r.status_code, "for", r.url)
                        with open(err_file, "w") as fout:
                            print(r, file=fout)
                except Exception as e:
                    print("error for", article["url"])
                    print(e)
                    with open(err_file, "w") as fout:
                        print(e, file=fout)


def parse_article(path):
    a = newspaper.Article(url="asdf")
    with open(path) as fin:
        a.set_html(fin.read())

    a.parse()
    return {
        'title': a.title,
        'text': a.text,
        'top_image': a.top_image,
        'images': list(a.images)
    }


def parse_all_articles(story_dir, include_images=False):
    for story in tqdm(os.listdir(story_dir)):
        for f in os.listdir(f"{story_dir}/{story}"):
            idx = f[0]
            if idx.isdigit() and f.endswith(".html"):
                path = f"{story_dir}/{story}/{f}"
                output = f"{story_dir}/{story}/{f[0]}.json"
                if os.path.exists(output):
                    continue

                try:
                    result = parse_article(path)
                    with open(f"{story_dir}/{story}/{idx}.json", "w") as fout:
                        print(json.dumps(result, ensure_ascii=False), file=fout)
                except Exception as e:
                    err_file = "f{story_dir}/{story}/{idx}.err"
                    print("error! see", err_file)
                    with open(err_file, "a") as fout:
                        print(e, file=fout)


def compile_dataset(story_dir, include_images=True):
    data = []
    for story in os.listdir(story_dir):
        if not os.path.exists(f"{story_dir}/{story}/story.json"):
            print("no story:", story)
            continue

        with open(f"{story_dir}/{story}/story.json") as fin:
            j = json.loads(fin.read())

        story_metadata = j['articles']
        articles = {}
        for f in os.listdir(f"{story_dir}/{story}"):
            if f[0].isdigit() and f.endswith(".json"):
                idx = int(f[0])
                with open(f"{story_dir}/{story}/{f}") as fin:
                    a = json.loads(fin.read())
                    if not include_images:
                        del a['top_image']
                        del a['images']
                    articles[idx] = a

        # put article info from story metadata into the individual articles
        for art_no in articles:
            articles[art_no]['side'] = story_metadata[art_no]['side']
            articles[art_no]['source'] = story_metadata[art_no]['source']
            articles[art_no]['url'] = story_metadata[art_no]['url']

        j['articles'] = articles
        j['date'] = story.split('.')[0]  # date in directory name

        if len(articles) > 0:
            data.append(j)
    return sorted(data, key=lambda x: x['date'])


def process_roundups(output_dir, delay):
    """
    Download roundups and store info in roundups.tsv
    """
    download_roundups(f"{output_dir}/roundups", delay)
    roundups = parse_roundups(f"{output_dir}/roundups")
    with open(f"{output_dir}/roundups.tsv", "w") as fout:
        for r in roundups:
            print('\t'.join(r), file=fout)


def main():
    parser = ArgumentParser()
    parser.add_argument("output_dir")
    parser.add_argument("-r", "--roundup", action="store_true",
                        help="download roundup files and create roundups.tsv")
    parser.add_argument("-s", "--scrape", action="store_true",
                        help="download story summary and articles, and parse them with newspaper")
    parser.add_argument("-c", "--compile", action="store_true",
                        help="compile all article text and data into a single jsonl file")
    parser.add_argument("--redownload", action="store_true",
                        help="download stories even if they already exist")
    parser.add_argument("--retry_errors", action="store_true",
                        help="try downloading stories and articles that previously errored")
    parser.add_argument("-d", "--delay", type=float, default=0.1,
                        help="seconds to sleep after each request")
    args = parser.parse_args()

    output_dir = args.output_dir
    story_dir = f"{output_dir}/story"
    os.makedirs(output_dir, exist_ok=True)

    if args.roundup:
        print("Downloading roundups")
        process_roundups(output_dir, args.delay)

    if args.scrape:
        print("Downloading stories")
        roundups = read_roundups(f"{output_dir}/roundups.tsv")
        download_stories(roundups, story_dir,
                         redownload=args.redownload, delay=args.delay)
        parse_all_stories(story_dir)
        print("Downloading articles. This might take a while...")
        download_articles(story_dir, redownload=args.redownload,
                          retry_errors=args.retry_errors, delay=args.delay)
        print("Parsing articles")
        parse_all_articles(story_dir)

    if args.compile:
        print("Compiling dataset into allsides.jsonl")
        data = compile_dataset(story_dir)
        with open(f"{output_dir}/allsides.jsonl", "w") as fout:
            for d in data:
                print(json.dumps(d, ensure_ascii=False), file=fout)


if __name__ == "__main__":
    main()
