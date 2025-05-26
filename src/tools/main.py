import plyvel
from typing import List, Optional
from pydantic import BaseModel
import json
from bs4 import BeautifulSoup


class RelatedItem(BaseModel):
    title: str
    url: str
    html_content: str

class NewsItem(BaseModel):
    title: str
    url: str
    full_page_content: str
    comes_from: str
    published_time: str
    comment_count: int
    tags: List[str]
    related: List[RelatedItem]




def html_to_paragraphs(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    paragraphs = [p.get_text(strip=True) for p in soup.find_all('p') if p.get_text(strip=True)]
    return paragraphs

def save_paragraphs(paragraphs: list[str], count: int):
    with open(f'extracted/{str(count).zfill(4)}.json', 'w+', encoding='utf-8') as f:
        json.dump(paragraphs, f, ensure_ascii=False)

if __name__ == "__main__":
    with plyvel.DB('data/news_content.lvdb') as db:
        count = 0
        all_paragraphs: list[str] = []
        for k, v in db.iterator():
            news_item = NewsItem.model_validate_json(str(v, encoding='utf-8'))
            html = news_item.full_page_content
            paragraphs = html_to_paragraphs(html)
            all_paragraphs.extend(paragraphs)
            count += 1
            if count % 1000 == 0:
                save_paragraphs(all_paragraphs, count)
                all_paragraphs = []
        #save the last part of the paragraphs to json file.
        save_paragraphs(all_paragraphs, count)
        
        print("done.")