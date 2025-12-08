import mysql.connector
import configparser
import os
from datetime import datetime


def create_database_and_table():
    """
    创建 nju_news 数据库，并创建统一的新闻数据表 news_all。
    从 config.ini 读取连接信息。
    """

    # 1. 加载配置文件
    config = configparser.ConfigParser()
    # 获取脚本所在的目录，以查找 config.ini
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.ini')

    if not os.path.exists(config_path):
        print(f"❌ 错误：配置文件未找到于 {config_path}")
        print("请确保 config.ini 存在于脚本同级目录。")
        return

    config.read(config_path, encoding='utf-8')

    try:
        DB_HOST = config.get('DATABASE', 'HOST')
        DB_USER = config.get('DATABASE', 'USER')
        DB_PASSWORD = config.get('DATABASE', 'PASSWORD')
        DB_NAME = config.get('DATABASE', 'DATABASE_NAME')
    except configparser.NoOptionError as e:
        print(f"❌ 配置错误：config.ini 中缺少必要的配置项：{e}")
        return

    # 2. 连接数据库服务器
    connection = None
    try:
        # 尝试连接到 MySQL 服务器（不指定数据库名）
        connection = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD
        )

        cursor = connection.cursor()

        # 3. 创建数据库
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4")
        cursor.execute(f"USE {DB_NAME}")

        TABLE_NAME = 'news_all'

        # 4. 创建新闻表 (news_all)
        # 字段列表已根据最新要求精简
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            
            -- 核心数据字段
            url VARCHAR(1000) NOT NULL COMMENT '网页地址',
            title VARCHAR(500) NOT NULL COMMENT '通知标题',
            publish_time DATE COMMENT '发布时间',
            content LONGTEXT COMMENT '内容描述',
            
            -- 新增或重命名字段
            image_links JSON COMMENT '图片链接列表 (JSON数组)',
            attachment_links JSON COMMENT '附件链接列表 (JSON数组)',
            
            -- 爬取和去重字段
            url_hash VARCHAR(32) UNIQUE NOT NULL COMMENT 'URL的MD5哈希值，用于去重',
            crawl_time DATETIME NOT NULL COMMENT '爬取时间',
            
            -- 【已移除 tag, created_at, updated_at】
            
            -- 索引
            UNIQUE KEY uk_url_hash (url_hash),
            INDEX idx_publish_time (publish_time),
            FULLTEXT KEY ft_title_content (title, content)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='统一新闻数据汇总表'
        """

        cursor.execute(create_table_query)
        print(f"\n✅ 统一新闻表 '{TABLE_NAME}' 创建成功或已存在。")

        # 5. 验证表结构
        print("\n--- 表结构 (news_all) ---")
        try:
            cursor.execute(f"DESCRIBE {TABLE_NAME}")
            max_col_len = 0
            columns = []
            for column in cursor.fetchall():
                col_name = (
                    column[0].decode('utf-8')
                    if isinstance(column[0], bytes)
                    else str(column[0])
                )
                col_type = (
                    column[1].decode('utf-8')
                    if isinstance(column[1], bytes)
                    else str(column[1])
                )
                columns.append((col_name, col_type, column[2]))
                max_col_len = max(max_col_len, len(col_name))

            # 格式化输出表结构
            header_format = f"| {{:<{max_col_len + 2}}} | {{:<15}} | {{:<5}} |"
            row_format = f"| {{:<{max_col_len + 2}}} | {{:<15}} | {{:<5}} |"

            print(header_format.format('字段名', '类型', 'NULL'))
            print('-' * (max_col_len + 25))

            for name, col_type, nullable in columns:
                print(row_format.format(name, col_type, nullable))
        except mysql.connector.Error as e:
            print(f"❌ 无法显示表结构: {e}")

        connection.commit()

    except mysql.connector.Error as e:
        print(f"❌ 数据库操作失败: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    create_database_and_table()
