import requests
from bs4 import BeautifulSoup
import urllib3
import re
import json
from urllib.parse import urljoin
import os
from datetime import datetime
import mysql.connector
import hashlib


class InfoExtractor:
    def __init__(self, tag, password):
        """初始化基本信息"""
        self.tag = tag
        self.url = f"https://xsxy.nju.edu.cn/sylm/{tag}/index.html"
        self.base_url = "https://xsxy.nju.edu.cn"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        }
        self.password = password

    def get_origin_html(self):
        """获取目录页html内容"""
        """None->html(str)"""
        try:
            response = requests.get(
                self.url, headers=self.headers, timeout=30
            )
            response.raise_for_status()
            html_content = response.text
            print("成功获取目录页HTML内容")
            return html_content
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            return None

    def extract_data(self, html_content):
        """使用BeautifulSoup提取JavaScript数据"""
        """html(str)->soup(BeautifulSoup)->json_data(list/dict)"""
        soup = BeautifulSoup(html_content, 'html.parser')
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and 'dataList' in script.string:
                # 找到包含dataList的脚本
                script_content = script.string
                # 使用更精确的正则表达式提取dataList
                patterns = [
                    r'var\s+dataList\s*=\s*(\[.*?\])\s*;',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, script_content, re.DOTALL)
                    if match:
                        json_str = match.group(1)
                        # 尝试直接解析
                        try:
                            data = json.loads(json_str)
                            print("JSON解析成功!")
                            return data
                        except json.JSONDecodeError:
                            print("未找到包含dataList的脚本")
                            return None

    def process_data(self, json_data):
        """处理提取的数据"""
        """json_data(list/dict)->news_items(list(dict{title,url,date,summary}))"""
        if not json_data:
            print("未找到数据列表")
            return []

        news_items = []

        # 处理不同的数据结构
        if isinstance(json_data, list):
            for item in json_data:
                if isinstance(item, dict):
                    # 直接包含新闻信息
                    if 'title' in item:
                        news_item = self.create_news_item(item)
                        if news_item:
                            news_items.append(news_item)
                    # 包含infolist
                    elif 'infolist' in item and isinstance(item['infolist'], list):
                        for news in item['infolist']:
                            news_item = self.create_news_item(news)
                            if news_item:
                                news_items.append(news_item)

        return news_items

    def normalize_url(self, url):
        """规范化URL，将相对路径转换为绝对路径"""
        if not url:
            return ""
        if url.startswith('http'):
            return url
        elif url.startswith('/'):
            return urljoin(self.base_url, url)
        else:
            return urljoin(self.url, url)

    def classify_url(self, url):
        """根据URL特征进行分类"""
        """str->int(1/2/3)"""
        if not url:
            return 0
        
        # 第一类：微信公众号文章
        if 'mp.weixin.qq.com' in url:
            return 1
        # 第二类：新生学院院内新闻
        elif 'xsxy.nju.edu.cn' in url and '/sylm/ynxw/' in url:
            return 2
        # 第三类：南京大学主站新闻
        elif 'www.nju.edu.cn' in url and '/info/' in url:
            return 3
        #第四类：admission.nju.edu.cn
        elif 'admission.nju.edu.cn' in url:
            return 3
        # 其他类型
        else:
            return 0

    def get_news_text(self, url):
        """根据URL类型使用不同策略提取新闻正文"""
        """str->str"""
        if not url:
            return ""
        
        category = self.classify_url(url)
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'  # 统一使用UTF-8编码
            soup = BeautifulSoup(response.text, 'html.parser')
            
            if category == 1:  # 微信公众号文章
                return self.extract_weixin_content(soup)
            elif category == 2:  # 新生学院院内新闻
                return self.extract_xsxy_content(soup)
            elif category == 3:  # 南京大学主站新闻
                return self.extract_main_site_content(soup)
            elif category == 4:  # admission.nju.edu.cn
                return self.extract_admission_content(soup)
            else:  # 其他类型，使用通用提取
                return self.extract_general_content(soup)
                
        except Exception as e:
            print(f"提取新闻正文失败 {url}: {e}")
            return ""

    def extract_weixin_content(self, soup):
        """提取微信公众号文章正文"""
        # 微信公众号文章正文通常在rich_media_content类中
        content_div = soup.find('div', class_='rich_media_content')
        if content_div:
            # 清理不必要的标签
            for element in content_div.find_all(['script', 'style', 'iframe']):
                element.decompose()
            text = content_div.get_text(separator='\n', strip=True)
            return text
        return ""

    def extract_xsxy_content(self, soup):
        """提取新生学院新闻正文"""
        # 主要查找content类的div
        content_div = soup.find('div', class_='content')
        
        if content_div:
            # 清理不必要的标签
            for element in content_div.find_all(['script', 'style', 'iframe', 'div.ctx-music', 'div.control']):
                element.decompose()
            
            # 提取所有段落文本
            paragraphs = []
            for p in content_div.find_all('p'):
                # 获取段落文本并清理
                text = p.get_text(strip=True)
                if text and not text.isspace() and text != "= $0":  # 跳过空段落和特殊字符
                    paragraphs.append(text)
            
            # 如果没有找到段落，尝试直接提取整个内容的文本
            if not paragraphs:
                text = content_div.get_text(separator='\n', strip=True)
                return text
            
            # 合并段落，用换行符分隔
            return '\n'.join(paragraphs)
        
        # 备用选择器
        content_selectors = [
            'div.m-ctx',
            'div.article-content',
            '.wp_articlecontent',
            'div#content'
        ]
        
        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                # 清理不必要的标签
                for element in content_div.find_all(['script', 'style', 'iframe', 'div.control']):
                    element.decompose()
                text = content_div.get_text(separator='\n', strip=True)
                return text
        
        return ""

    def extract_main_site_content(self, soup):
        """提取南京大学主站新闻正文"""
        # 主站新闻可能在特定的容器中
        content_selectors = [
            'div.article-content',
            'div.content',
            '.wp_articlecontent',
            'div#content'
        ]
        
        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                # 清理不必要的标签
                for element in content_div.find_all(['script', 'style', 'iframe']):
                    element.decompose()
                text = content_div.get_text(separator='\n', strip=True)
                return text
        return ""

    def extract_general_content(self, soup):
        """通用正文提取方法"""
        # 尝试提取article标签或主要内容区域
        content_selectors = [
            'article',
            'div.article',
            'div.content',
            'div.main-content',
            'div.post-content',
            'div.entry-content'
        ]
        
        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                # 清理不必要的标签
                for element in content_div.find_all(['script', 'style', 'iframe']):
                    element.decompose()
                text = content_div.get_text(separator='\n', strip=True)
                return text
        return ""

    def create_news_item(self, news_dict):
        """从字典创建新闻项 - 增加正文内容字段"""
        if not isinstance(news_dict, dict):
            return None

        title = news_dict.get('title', '').strip()
        if not title:  # 如果没有标题，跳过
            return None
        
        url = self.normalize_url(news_dict.get('url', ''))
        
        # 创建新闻项时提取正文内容
        news_item = {
            'title': title,
            'url': url,
            'date': news_dict.get('daytime', news_dict.get('date', '')),
            'summary': (
                news_dict.get('summary', '')[:100] + '...'
                if news_dict.get('summary')
                else ''
            ),
            'category': self.classify_url(url),
            'content': ""  # 初始化为空，稍后填充
        }
        
        # 只有在有有效URL时才提取正文
        if url and url.startswith('http'):
            news_item['content'] = self.get_news_text(url)
        
        return news_item

    def save_to_json(self, news_data, filename=None):
        """将每篇新闻数据保存为单独的JSON文件，支持更新旧文件"""
        # 创建文件夹
        script_dir = os.path.dirname(os.path.abspath(__file__))
        news_dir = os.path.join(script_dir, self.tag)
        
        # 确保文件夹存在
        if not os.path.exists(news_dir):
            os.makedirs(news_dir)
            print(f"创建目录: {news_dir}")
        
        saved_count = 0
        updated_count = 0
        
        for news_item in news_data:
            # 使用URL的MD5哈希作为唯一标识符，确保文件名稳定
            url_hash = hashlib.md5(news_item['url'].encode('utf-8')).hexdigest()[:8]
            
            # 文件名格式：标题_时间_hash前8位
            title_part = re.sub(r'[^\w\u4e00-\u9fa5]', '_', news_item['title'])
            date_part = news_item['date'].replace('-', '') if news_item['date'] else 'nodate'
            
            # 创建文件名：标题_时间_hash前8位
            filename = f"{date_part}_{title_part}_{url_hash}.json"
            file_path = os.path.join(news_dir, filename)
            
            # 检查文件是否已存在
            file_exists = os.path.exists(file_path)
            
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(news_item, f, ensure_ascii=False, indent=2)
                
                if file_exists:
                    updated_count += 1
                else:
                    saved_count += 1
                    
            except Exception as e:
                print(f"保存文件 {filename} 时出错: {e}")
        
        print(f"成功保存 {saved_count} 篇新新闻，更新 {updated_count} 篇已有新闻")
        print(f"文件保存目录: {news_dir}")
        return saved_count + updated_count
    
    def save_to_database(self, news_data):
        """将新闻数据保存到MySQL数据库 - 支持更新重复新闻"""
        try:
            connection = mysql.connector.connect(
                host='localhost',
                user='root',
                password=self.password,
                database='nju_news'
            )
            cursor = connection.cursor()

            # 创建临时表用于存储URL哈希值
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_urls (
                    url_hash VARCHAR(32) PRIMARY KEY,
                    url VARCHAR(1000)
                )
            """)
            
            # 在插入前先去重
            unique_urls = {}
            for item in news_data:
                url_hash = hashlib.md5(item['url'].encode('utf-8')).hexdigest()
                if url_hash not in unique_urls:
                    unique_urls[url_hash] = item['url']
            
            # 插入去重后的URL哈希值
            url_hashes = [(hash, url) for hash, url in unique_urls.items()]
            
            if url_hashes:
                cursor.executemany(
                    "INSERT IGNORE INTO temp_urls (url_hash, url) VALUES (%s, %s)",
                    url_hashes
                )
            
            # 使用INSERT ... ON DUPLICATE KEY UPDATE语句
            insert_query = f"""
            INSERT INTO news_{self.tag} (title, url, date, summary, content, category, url_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                date = VALUES(date),
                summary = VALUES(summary),
                content = VALUES(content),
                category = VALUES(category),
                updated_at = CURRENT_TIMESTAMP
            """

            inserted_count = 0
            updated_count = 0
            
            # 处理去重后的数据
            processed_urls = set()
            for item in news_data:
                url_hash = hashlib.md5(item['url'].encode('utf-8')).hexdigest()
                
                # 跳过已处理的URL
                if url_hash in processed_urls:
                    continue
                processed_urls.add(url_hash)
                
                try:
                    cursor.execute(insert_query, (
                        item['title'],
                        item['url'],
                        item['date'],
                        item['summary'],
                        item['content'],
                        item['category'],
                        url_hash
                    ))
                    
                    if cursor.rowcount == 1:  # 新插入的行
                        inserted_count += 1
                    elif cursor.rowcount == 2:  # 更新的行
                        updated_count += 1
                        
                except mysql.connector.Error as err:
                    print(f"插入/更新新闻失败: {err}")
                    continue

            connection.commit()
            print(f"数据库操作完成: 新增 {inserted_count} 条, 更新 {updated_count} 条")
            
        except mysql.connector.Error as err:
            print(f"数据库错误: {err}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def run(self):
        """运行提取程序"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        print(f"工作目录已设置为: {os.getcwd()}")
        
        html = self.get_origin_html()
        if not html:
            return []

        data_list = self.extract_data(html) 
        if not data_list:
            print("未能提取到数据")
            return []

        news = self.process_data(data_list)
        print(f"\n=== 最终结果 ===")
        print(f"成功提取 {len(news)} 篇新闻")

        # 保存数据到单独的JSON文件
        if news:
            saved_count = self.save_to_json(news_data=news)
            print(f"实际保存 {saved_count} 篇新闻到{self.tag}文件夹")
        
        # 保存到数据库
        if news:
            self.save_to_database(news_data=news)
        
        return news

if __name__ == "__main__":
    tags=["fmxw","tzgg","ynxw","xyld","dss","xss",]
    for tag in tags:
        extractor = InfoExtractor(tag=tag, password='061112')
        news = extractor.run()
        # 保存到数据库
        if news:
            extractor.save_to_database(news_data=news)