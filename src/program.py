import asyncio

# import crawlee.autoscaling
import crawlee.beautifulsoup_crawler
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
import crawlee
import crawlee.storages
import bs4
import plyvel
import typing
import json
import requests
import pydantic
import time
import traceback

class NewsItem(pydantic.BaseModel):
    # "id": "26829137",
    # "title": "ﺋﯘﻛﺮﺍﺋﯩﻨﺎﻟﯩﻘﻼﺭ ﺯﯦﻤﯩﻦ ﺑﻪﺩﯨﻠﯩﮕﻪ ﺗﯩﻨﭽﻠﯩﻘﻘﺎ ﺋﯧﺮﯨﺸﯩﺸﻨﻰ ﺧﺎﻻﻳﺪﯨﻐﺎﻥ ﺑﻮﻟﯘﭖ ﻗﺎﻟﻤﺎﻗﺘﺎ",
    # "thumb": "https://cdnf.nur.cn/uploadfile/2024/0724/072419254719500000010094.jpgthumb_210_150.jpg",
    # "date_txt": "21 ﺳﺎﺋﻪﺕ ﺋﯩﻠﮕﯩﺮﻯ",
    # "bahanum": "0",
    # "copyfrom": "تېڭشۈن تورى",
    # "type": "news",
    # "url": "/news/2024/07/26829137.shtml"

    id: str
    title: str
    thumb: list | None
    date_txt: str | None
    bahanum: str | None
    copyfrom: str | None
    type: str | None
    url: str
    crawled: bool

class NewsContentRelate(pydantic.BaseModel):
    title: str
    url: str
    html_content: str

class NewsContent(pydantic.BaseModel):
    title: str
    url: str
    full_page_content:  str
    comes_from: str | None
    published_time: str
    comment_count: int
    tags: list[str]
    related: list[NewsContentRelate]



class Storage:
    def __init__(self, database_location: str):
        self.db = plyvel.DB(database_location, create_if_missing=True)
    def put(self, key: str, value: str):
        self.db.put(key.encode(), value.encode())
    def get(self, key: str) -> str | None:
        value = self.db.get(key.encode())
        return None if value is None else value.decode()
    def delete(self, key: str) -> None:
        self.db.delete(key.encode())
    def close(self):
        self.db.close()

news_category_db = Storage("./data/news_category.lvdb")
news_list_db = Storage("./data/news_list.lvdb")
news_contents_db = Storage("./data/news_content.lvdb")

import warnings
warnings.filterwarnings("error", category=UserWarning, module="pydantic")

async def main(craw_news_category: bool, clean_redundant_category, craw_news_list_from_each_category: bool, craw_news_content: bool) -> None:
    # crawler = PlaywrightCrawler(
    #     browser_type="chromium",
    #     headless=True,
    #     max_requests_per_crawl=100000,
        # concurrency_settings=crawlee.autoscaling.ConcurrencySettings(min_concurrency=10)w
    # )

    crawler = BeautifulSoupCrawler(
        concurrency_settings=crawlee.ConcurrencySettings(max_concurrency=3, max_tasks_per_minute=60 * 3),
        use_session_pool=True,
        max_request_retries=5,
        max_requests_per_crawl=10000,
    )

    crawle_news_category_storage = await crawlee.storages.Dataset.open(name="news_Category")

    @crawler.router.handler("News")
    async def handle_news_response(context: BeautifulSoupCrawlingContext):
        # print("*************************************")
        try:
            context.log.info(f"processing {context.request.url} ...")

            title, content = context.soup.select_one(".tt"), context.soup.select_one(".mazmun")
            
            # if title is not None and content is not None:
            data = {
                "title": title.text,
                "url": context.request.url,
                "full_page_content": content.decode_contents()
            }

            top_view = context.soup.select_one(".view-top")
            top_view_children = [i for i in list(top_view.children) if i != "\n"]
            top_view_children: list[bs4.element.Tag]

            data["comes_from"] = list(top_view_children[0].children)[1].text
            data["published_time"] = list(top_view_children[1].children)[1].text
            data["comment_count"] = int(list(top_view_children[2].children)[0].text.replace(" ", ""))

            bottom_view = context.soup.select_one(".view-bottom .v1")
            bottom_view_children = [list(i.children) for i in list(bottom_view.children)[1:] if i != "\n"]
            bottom_view_children = [i[1] for i in bottom_view_children]

            data["tags"] = [i.text for i in bottom_view_children]

            related_1 = context.soup.select(".related-left ul li a")
            related_2 = context.soup.select(".view1 #news_list_item .list-li1 a")
            related = [*related_1, *related_2]
            related = [{"url": i.attrs["href"], "html_content": i.decode(), "title": i.find("h4").text} for i in related]
            data["related"] = related

            
            model = NewsContent.model_validate(data)
            news_contents_db.put(model.url, model.model_dump_json())
            news_item = context.request.user_data["news_object"]
            news_item = news_item if type(news_item) is NewsItem else NewsItem.model_validate(news_item)
            news_item: NewsItem
            news_item.crawled = True
            news_list_db.put(news_item.url, news_item.model_dump_json())
            await context.push_data(data=model.model_dump())
            # await context.enqueue_links(include=[crawlee.Glob("https://nur.cn/*")])
            await asyncio.sleep(0.3)
        except Exception as e:
            traceback.print_exc()
            print(f"{e}")

    @crawler.router.handler("NewsList")
    async def handle_lists(context: BeautifulSoupCrawlingContext):
        all_scripts = context.soup.find_all("script", attrs={"type": "text/javascript"})
        all_scripts: bs4.ResultSet[bs4.Tag]
        all_scripts = [i for i in all_scripts if i.text.count("Load_ajax_auto_more(") > 0]
        if len(all_scripts) == 0:
            return
        type_id_script = all_scripts[0].text
        start, end = type_id_script.index("Load_ajax_auto_more("), -1
        end = type_id_script.index(");", start)
        parameter = type_id_script[start:end]
        parameter = parameter.removeprefix("Load_ajax_auto_more(").replace("\"", "'").replace("'", "").split(",")
        action, category_id = parameter[0].split("?")[1].split("&")[0].split("=")[1], parameter[3].split("&")[1].split("=")[1]

        print(f"category={category_id}, action={action}")

        news_category_db.put(category_id, json.dumps({
            "index": context.request.user_data["index"],
            "id": category_id,
            "crawled": False,
            "last_page_index": 1,
        }))
        await asyncio.sleep(0.3)

    if craw_news_category:
        for i in range(600):
            url = f"https://nur.cn/lists/{i}/1.shtml"
            request = crawlee.Request.from_url(url=url, label="NewsList")
            request.user_data["index"] = i
            await crawler.run([request])
            print(i)
        category_list = [k.decode() for k, v in news_category_db.db]
        await crawle_news_category_storage.push_data([{"categories": category_list}])
        print("News categories are collected!")

    def retrieve_news_list(category_id: str, page: int, action: str) -> list[NewsItem]:
        url = f"https://nur.cn/index.php?m=home&a=ajax2"
        response = requests.request(url=url, method="post", data={"action": action, "page":page, "catid": category_id})
        news_list_response = response.json()
        news_list_response: list[NewsItem]
        news_list: list[NewsItem] = []
        for news in news_list_response:
            if type(news["thumb"]) is str:
                news["thumb"] = [news["thumb"]]
            news["crawled"] = False
            news["last_page_index"] = 1
            news = NewsItem.model_validate(news)
            news_list.append(news)
        return news_list

    if clean_redundant_category:
        for i, (k, v) in enumerate([(k.decode(), v.decode()) for k, v in news_category_db.db]):
            k: str
            news_list_response = retrieve_news_list(category_id=k, page=1, action="lists")
            is_redundant_category = True
            for news in news_list_response:
                existing_value = news_list_db.get(f"{news.url}")
                is_redundant_category &= existing_value is not None
                news_list_db.put(news.url, news.model_dump_json())
            is_redundant_category &= len(news_list_response) > 0
            if is_redundant_category:
                news_category_db.delete(k)
                print(f"{v} Redundant so removed!")
            await asyncio.sleep(0.3)
            print(f"Working with {i}...")
        categories = [k.decode() for k, v in news_category_db.db]
        print(f"{len(categories)} after clean.")

    # for i, news in enumerate([NewsItem.model_validate_json(v.decode()) for k, v in news_list_db.db]):
    #     print(f"{i}: {news.url} {news.date_txt},\n   {news.title}")
    # return
    
    if craw_news_list_from_each_category:
        categories = [(k.decode(), json.loads(v.decode()), "lists") for k, v in news_category_db.db]
        categories: list[tuple[str, dict, str]]
        categories.append(("home", {
            "index": len(categories),
            "id": "home",
            "crawled": False,
            "last_page_index": 1,
        }, "home"))
        for i, (category_id, category_value, action) in enumerate(categories):
            category_id: str
            category_value: dict[str, typing.Any]
            if category_value["crawled"]:
                continue
            if category_value.get("last_page_index") is None:
                category_value["last_page_index"] = 1
            
            duplicated_count_threshold = 10
            duplicated_times = 0
            while True:
                elapsed_time = time.time_ns()
                news_list = retrieve_news_list(category_id, category_value["last_page_index"], action=action)
                elapsed_time = time.time_ns() - elapsed_time

                if len(news_list) == 0:
                    category_value["crawled"] = True
                    news_category_db.put(category_id, json.dumps(category_value))
                    print(f"empty news list returned from {category_id} so jumping to next category.")
                    break
                print(f"Processing category of {i}/{len(categories)} with page: {category_value["last_page_index"]} in {int(elapsed_time / 1000 / 1000)}ms")

                exists = True
                for news in news_list:
                    exists &= news_list_db.get(news.url) is not None
                    news_list_db.put(news.url, news.model_dump_json())
                if exists:
                    duplicated_times += 1
                    print("Guessing page is overflow!")
                else:
                    duplicated_times = 0
                if duplicated_times >= duplicated_count_threshold:
                    category_value["crawled"] = True
                    news_category_db.put(category_id, json.dumps(category_value))
                    print(f"page overflow is confirmed of category: {category_id} and jump to processing of next category!")
                    break
                news_category_db.put(category_id, json.dumps(category_value))
                category_value["last_page_index"] += 1
    async def crawle_news_from_news_list(continue_from_break_point: bool, chunck_size = 64):
        force = not continue_from_break_point
        T = typing.TypeVar("T")
        def slice_list_into_chunks(list: list[T], chunck_size: int) -> list[list[T]]:
            ret_val = []
            for i in range(0, len(list), chunck_size):
                ret_val.append(list[i:i + chunck_size])
            return ret_val
        
        news_list = [NewsItem.model_validate_json(v.decode()) for k, v in news_list_db.db]
        total_count = len(news_list)
        if force:
            for i in news_list:
                i.crawled = False
                news_list_db.put(i.url, i.model_dump_json())
        news_list = [i for i in news_list if i.crawled == False]
        # for ni in news_list:
        #     try:
        #         temp = ni.model_dump_json()
        #     except Exception as e:
        #         print(e)
        chunks = slice_list_into_chunks(news_list, chunck_size)
        for i, chunk in enumerate(chunks):
            requests: list[crawlee.Request] = []
            for item in chunk:
                request = crawlee.Request.from_url(f"https://nur.cn{item.url}", label="News")
                request.user_data["news_object"] = item.model_dump()
                requests.append(request)
            await crawler.run(requests=requests)

            (total_count, completed_count) = (total_count, (total_count - len(news_list) + ((i + 1) * chunck_size)))
            print(f"*** [{((completed_count / total_count) * 100.0):.3f}%] {completed_count}/{total_count} news are crawed!\n\n")
        
    if craw_news_content:
        await crawle_news_from_news_list(continue_from_break_point=True, chunck_size=64)
    print("Over!")

if __name__ == "__main__":
    asyncio.run(main(
        craw_news_category=True,
        clean_redundant_category=True,
        craw_news_list_from_each_category=True,
        craw_news_content=True
    ))
