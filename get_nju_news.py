import requests
from bs4 import BeautifulSoup
import re
import json
import logging
import hashlib
import configparser
from urllib.parse import urljoin
from datetime import datetime
import mysql.connector
from mysql.connector import Error

# ==========================================
# 日志配置：记录到文件和控制台
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log", encoding='utf-8'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class CrawlerModule:
    def __init__(self, config_path='config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf-8')

        # 数据库配置
        self.db_config = {
            'host': self.config.get('DATABASE', 'HOST'),
            'user': self.config.get('DATABASE', 'USER'),
            'password': self.config.get('DATABASE', 'PASSWORD'),
            'database': self.config.get('DATABASE', 'DATABASE_NAME'),
        }

        # 爬虫逻辑配置
        tags_str = self.config.get('CRAWLER', 'TAGS')
        self.tags = [t.strip() for t in tags_str.split(',') if t.strip()]
        self.base_url = "https://xsxy.nju.edu.cn"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."
        }

        # 选择器与附件后缀
        self.content_selectors = [
            'div.rich_media_content',
            'div.article-content',
            'div.content',
            '.wp_articlecontent',
        ]
        self.file_extensions = (
            '.pdf',
            '.doc',
            '.docx',
            '.xls',
            '.xlsx',
            '.zip',
            '.rar',
        )

    def _get_html(self, url):
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            resp.raise_for_status()
            resp.encoding = 'utf-8'
            return resp.text
        except Exception as e:
            logger.error(f"请求失败 {url}: {e}")
            return None

    def _extract_detail(self, url):
        """提取正文、图片列表、附件列表"""
        if 'admission.nju.edu.cn' in url:
            return "", [], []

        html = self._get_html(url)
        if not html:
            return "", [], []

        soup = BeautifulSoup(html, 'html.parser')
        container = None
        for s in self.content_selectors:
            container = soup.select_one(s)
            if container:
                break

        content_text, images, attachments = "", [], []
        if container:
            # 清除干扰
            for tag in container.find_all(['script', 'style', 'iframe']):
                tag.decompose()

            # 提取图片
            images = [
                urljoin(url, img.get('src') or img.get('data-src'))
                for img in container.find_all('img')
                if (img.get('src') or img.get('data-src'))
            ]

            # 提取附件
            for link in container.find_all('a', href=True):
                href = link.get('href')
                if any(href.lower().endswith(ext) for ext in self.file_extensions):
                    attachments.append(
                        {'name': link.get_text(strip=True), 'url': urljoin(url, href)}
                    )

            # 提取正文
            p_tags = container.find_all('p')
            content_text = (
                "\n".join(
                    [p.get_text(strip=True) for p in p_tags if p.get_text(strip=True)]
                )
                if p_tags
                else container.get_text("\n", strip=True)
            )

        return content_text, images, attachments

    def _get_list_data(self, tag):
        """获取目录页数据"""
        index_url = f"{self.base_url}/sylm/{tag}/index.html"
        html = self._get_html(index_url)
        if not html:
            return []

        match = re.search(r'var\s+dataList\s*=\s*(\[.*?\])\s*;', html, re.DOTALL)
        if not match:
            return []

        try:
            raw_data = json.loads(match.group(1))
            news_list = []
            for item in raw_data:
                entries = item.get('infolist', [item]) if isinstance(item, dict) else []
                for e in entries:
                    if isinstance(e, dict) and 'title' in e:
                        news_list.append(e)
            return news_list
        except:
            return []

    def save_to_db(self, news_items):
        """
        适配新字段的数据库存入逻辑：
        1. 初始插入时：process_status=0, match_type=0, matched_keywords=[]
        2. 更新时：若内容变化，重置所有状态字段
        """
        if not news_items:
            return

        # 适配新字段的 SQL
        query = """
        INSERT INTO news_all (
            url, title, publish_time, content, 
            image_links, attachment_links, url_hash, crawl_time,
            process_status, match_type, matched_keywords
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            publish_time = VALUES(publish_time),
            image_links = VALUES(image_links),
            attachment_links = VALUES(attachment_links),
            crawl_time = VALUES(crawl_time),
            -- 核心逻辑：如果内容改变，重置所有处理状态
            match_type = IF(content != VALUES(content), 0, match_type),
            matched_keywords = IF(content != VALUES(content), '[]', matched_keywords),
            process_status = IF(content != VALUES(content), 0, process_status),
            content = VALUES(content)
        """

        conn = None
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()

            data_to_insert = []
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for item in news_items:
                url_hash = hashlib.md5(item['url'].encode()).hexdigest()
                data_to_insert.append(
                    (
                        item['url'],
                        item['title'],
                        item.get('date'),
                        item['content'],
                        json.dumps(item['images']),
                        json.dumps(item['attachments']),
                        url_hash,
                        now,
                        0,  # process_status: 初始为 0
                        0,  # match_type: 初始为 0
                        '[]',  # matched_keywords: 初始为空 JSON 数组
                    )
                )

            cursor.executemany(query, data_to_insert)
            conn.commit()
            logger.info(f"数据库同步完成，处理记录数: {len(news_items)}")
        except Error as e:
            logger.error(f"数据库同步失败: {e}")
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()

    def run(self):
        logger.info(">>> 启动爬虫模块")
        all_news = []
        for tag in self.tags:
            logger.info(f"正在爬取栏目: {tag}")
            items = self._get_list_data(tag)
            for i in items:
                url = urljoin(self.base_url, i.get('url', ''))
                content, imgs, atts = self._extract_detail(url)
                all_news.append(
                    {
                        'title': i.get('title', '').strip(),
                        'url': url,
                        'date': i.get('daytime') or i.get('date'),
                        'content': content,
                        'images': imgs,
                        'attachments': atts,
                    }
                )
        self.save_to_db(all_news)
        logger.info(">>> 爬虫模块任务结束")


if __name__ == "__main__":
    CrawlerModule().run()
