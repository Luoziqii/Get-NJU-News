# TODO:完成新闻图片与附件url的提取与保存功能
# TODO:忽略admission.nju.edu.cn的新闻正文提取(已完成)
# TODO:password和tag改为从配置文件读取
# TODO:增加日志记录功能，替代print
# TODO:美化代码结构（哭），增加注释
# TODO：添加requerements.txt文件

import requests
from bs4 import BeautifulSoup
import re
import json
from urllib.parse import urljoin
import os
from datetime import datetime
import mysql.connector
import hashlib
import configparser


def clean_and_extract_text(soup, selectors):
    """根据选择器列表，清理并提取正文文本"""
    for selector in selectors:
        content_div = soup.select_one(selector)
        if content_div:
            # 清理不必要的标签
            for element in content_div.find_all(
                ['script', 'style', 'iframe', 'div.ctx-music', 'div.control']
            ):
                element.decompose()

            # 尝试提取段落，如果失败则提取所有文本
            paragraphs = [
                p.get_text(strip=True)
                for p in content_div.find_all('p')
                if p.get_text(strip=True)
            ]

            if (
                paragraphs and len("".join(paragraphs)) > 50
            ):  # 确保不是只有几个字符的空内容
                return '\n'.join(paragraphs)
            else:
                return content_div.get_text(separator='\n', strip=True)
    return ""


class InfoExtractor:
    """负责从特定 tag 页面提取新闻列表和正文内容，不负责保存。"""

    # 集中定义选择器
    WEIXIN_SELECTORS = ['div.rich_media_content']
    NJU_SELECTORS = [
        'div.article-content',
        'div.content',
        '.wp_articlecontent',
        'div#content',
        'div.m-ctx',
        'article',
        'div.post-content',
        'div.entry-content',
    ]

    def __init__(self, tag):
        self.tag = tag
        self.url = f"https://xsxy.nju.edu.cn/sylm/{tag}/index.html"
        self.base_url = "https://xsxy.nju.edu.cn"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }

    def get_origin_html(self):
        """获取目录页 HTML 内容"""
        try:
            response = requests.get(
                self.url, headers=self.headers, timeout=15
            )  # 缩短超时时间
            response.raise_for_status()
            print(f"[{self.tag}] 成功获取目录页HTML")
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"[{self.tag}] 请求失败: {e}")
            return None

    def extract_data(self, html_content):
        """使用 BeautifulSoup 提取 JavaScript 中的 dataList"""
        soup = BeautifulSoup(html_content, 'html.parser')
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and 'dataList' in script.string:
                match = re.search(
                    r'var\s+dataList\s*=\s*(\[.*?\])\s*;', script.string, re.DOTALL
                )
                if match:
                    try:
                        data = json.loads(match.group(1))
                        print(f"[{self.tag}] JSON解析成功")
                        return data
                    except json.JSONDecodeError:
                        print(f"[{self.tag}] JSON解析失败")
                        return None
        print(f"[{self.tag}] 未找到包含 dataList 的脚本")
        return None

    def normalize_url(self, url):
        """规范化 URL，将相对路径转换为绝对路径"""
        if not url:
            return ""
        # 使用 urljoin 统一处理相对路径
        return urljoin(self.base_url, url)

    def classify_url(self, url):
        """根据 URL 特征进行分类"""
        if not url:
            return 0
        if 'mp.weixin.qq.com' in url:
            return 1  # 微信公众号
        if 'admission.nju.edu.cn' in url:
            return 4  # 忽略神秘招生网链接
        if 'nju.edu.cn' in url:
            return 3  # 南京大学链接
        return 0  # 其他

    def get_news_text(self, url):
        """根据 URL 类型使用不同策略提取新闻正文"""
        category = self.classify_url(url)
        if category == 4:
            return ""  # 忽略

        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')

            if category == 1:  # 微信公众号
                return clean_and_extract_text(soup, self.WEIXIN_SELECTORS)
            elif category == 3:  # 南京大学链接
                return clean_and_extract_text(soup, self.NJU_SELECTORS)
            else:  # 通用/其他
                return clean_and_extract_text(
                    soup, self.NJU_SELECTORS + self.WEIXIN_SELECTORS
                )  # 尝试所有选择器

        except Exception as e:
            # print(f"[{self.tag}] ❌ 提取正文失败 {url}: {e}") # 过于频繁，改为静默失败
            return ""

    def create_news_item(self, news_dict):
        """从字典创建新闻项，并提取正文内容"""
        title = news_dict.get('title', '').strip()
        if not title:
            return None

        url = self.normalize_url(news_dict.get('url', ''))

        news_item = {
            'tag': self.tag,
            'title': title,
            'url': url,
            'date': news_dict.get('daytime', news_dict.get('date', '')),
            'summary': (
                news_dict.get('summary', '')[:100] + '...'
                if news_dict.get('summary')
                else ''
            ),
            'category': self.classify_url(url),
            'content': "",
        }

        if url and url.startswith('http'):
            news_item['content'] = self.get_news_text(url)

        return news_item

    def run(self):
        """运行提取程序，返回提取到的新闻列表"""
        html = self.get_origin_html()
        if not html:
            return []

        data_list = self.extract_data(html)
        if not data_list:
            return []

        news_items = []
        # 统一处理数据结构：无论是一级列表还是包含 infolist 的二级结构
        raw_list = []
        for item in data_list:
            if (
                isinstance(item, dict)
                and 'infolist' in item
                and isinstance(item['infolist'], list)
            ):
                raw_list.extend(item['infolist'])
            elif isinstance(item, dict) and 'title' in item:
                raw_list.append(item)

        for news in raw_list:
            news_item = self.create_news_item(news)
            if news_item:
                news_items.append(news_item)

        print(f"[{self.tag}]  提取并处理 {len(news_items)} 篇新闻")
        return news_items


from datetime import datetime
import os
import mysql.connector


class NewsAggregator:
    """负责协调所有标签的爬取、去重和统一保存/数据库操作。"""

    def __init__(self, config):
        """初始化配置和数据存储"""
        # 数据库配置
        self.db_host = config.get('DATABASE', 'HOST')
        self.db_user = config.get('DATABASE', 'USER')
        self.db_password = config.get('DATABASE', 'PASSWORD')
        self.db_name = config.get('DATABASE', 'DATABASE_NAME')
        self.db_table = 'news_all'  # 统一保存到 news_all 表

        # 爬虫配置
        tags_str = config.get('CRAWLER', 'TAGS')
        self.tags = [tag.strip() for tag in tags_str.split(',') if tag.strip()]

        self.all_news = []
        self.unique_urls = set()

    def crawl_all_tags(self):
        """遍历所有 tag，提取新闻，并进行内存去重"""
        total_extracted = 0
        for tag in self.tags:
            extractor = InfoExtractor(tag=tag)
            news_list = extractor.run()
            total_extracted += len(news_list)

            # 内存去重：使用 URL 的 MD5 哈希作为唯一标识
            for item in news_list:
                url_hash = hashlib.md5(item['url'].encode('utf-8')).hexdigest()

                if url_hash not in self.unique_urls:
                    self.unique_urls.add(url_hash)
                    item['url_hash'] = url_hash
                    self.all_news.append(item)

        print(f"\n==========================================")
        print(
            f"✅ 汇总完成！总提取 {total_extracted} 篇，去重后得到 {len(self.all_news)} 篇唯一新闻。"
        )
        print(f"==========================================\n")
        return self.all_news

    def save_to_single_json(self):
        """将所有新闻数据保存为一个统一的 JSON 文件到当前目录"""
        if not self.all_news:
            print("⚠️ 没有新闻数据可保存到 JSON 文件。")
            return 0

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"all_tags_news_{timestamp}.json"

        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, filename)

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.all_news, f, ensure_ascii=False, indent=2)

            print(f"💾 成功保存 {len(self.all_news)} 篇新闻到汇总文件: {filename}")
            return len(self.all_news)

        except Exception as e:
            print(f"❌ 保存汇总文件 {filename} 时出错: {e}")
            return 0

    def save_to_database(self):
        """
        将新闻数据保存到 MySQL 数据库（使用 UPSERT 逻辑）。
        已移除 tag, created_at, updated_at 字段的存储。
        """
        if not self.all_news:
            return

        try:
            connection = mysql.connector.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
            )
            cursor = connection.cursor()

            # --- 【改动 1】：新的 SQL 语句，移除 tag, created_at, updated_at ---
            insert_query = f"""
            INSERT INTO {self.db_table} (
                url, title, publish_time, content, 
                image_links, attachment_links, url_hash, crawl_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                publish_time = VALUES(publish_time),
                content = VALUES(content),
                crawl_time = VALUES(crawl_time)
            """

            # --- 准备数据映射 ---
            data_to_insert = []
            current_crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for item in self.all_news:
                # 默认值处理
                image_links = json.dumps([])
                attachment_links = json.dumps([])
                publish_time = item.get('date') if item.get('date') else None

                # --- 【改动 2】：数据元组，移除 tag 字段的数据 ---
                data_to_insert.append(
                    (
                        item['url'],  # url
                        item['title'],  # title
                        publish_time,  # publish_time (原 date)
                        item['content'],  # content
                        image_links,  # image_links
                        attachment_links,  # attachment_links
                        item['url_hash'],  # url_hash (用于去重)
                        current_crawl_time,  # crawl_time (新生成)
                    )
                )

            # --- 执行插入 ---
            cursor.executemany(insert_query, data_to_insert)

            connection.commit()
            print(
                f"🖥️ 数据库操作完成：成功处理 {len(self.all_news)} 条新闻到 {self.db_table} 表。"
            )

        except mysql.connector.Error as err:
            print(f"❌ 数据库错误: {err}")
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()

    def run(self):
        """主执行流程"""
        self.crawl_all_tags()
        self.save_to_single_json()
        self.save_to_database()


if __name__ == "__main__":

    # 1. 加载配置文件
    config = configparser.ConfigParser()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.ini')

    if not os.path.exists(config_path):
        print(f"❌ 错误：配置文件未找到于 {config_path}")
        exit(1)

    config.read(config_path, encoding='utf-8')
    print("✅ 配置文件读取成功")

    # 2. 启动聚合器
    try:
        aggregator = NewsAggregator(config=config)
        aggregator.run()
    except configparser.NoOptionError as e:
        print(f"❌ 配置错误：config.ini 中缺少必要的配置项：{e}")
    except Exception as e:
        print(f"❌ 发生未知错误: {e}")
