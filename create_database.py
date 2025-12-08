import mysql.connector

def create_news_table(tag):
    """创建新闻数据表，包含正文内容和分类字段，支持更新"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='061112'
        )
        
        cursor = connection.cursor()
        
        # 创建数据库
        cursor.execute("CREATE DATABASE IF NOT EXISTS nju_news CHARACTER SET utf8mb4")
        cursor.execute("USE nju_news")
        
        # 创建新闻表 - 增加url_hash和updated_at字段
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS news_{tag} (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(500) NOT NULL COMMENT '新闻标题',
            url VARCHAR(1000) NOT NULL COMMENT '新闻链接',
            date DATE COMMENT '发布日期',
            summary TEXT COMMENT '新闻摘要',
            content LONGTEXT COMMENT '新闻正文',
            category TINYINT DEFAULT 0 COMMENT '新闻分类: 1-微信 2-新生学院 3-主站 0-其他',
            url_hash VARCHAR(32) UNIQUE COMMENT 'URL哈希值，用于去重',  -- 新增唯一索引字段
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',  -- 新增更新时间字段
            
            INDEX idx_title (title(100)),
            INDEX idx_date (date),
            INDEX idx_category (category),
            INDEX idx_created_at (created_at),
            INDEX idx_updated_at (updated_at),
            INDEX idx_url (url(100)),
            INDEX idx_url_hash (url_hash),  -- 为url_hash添加索引
            FULLTEXT KEY ft_title_summary (title, summary),
            FULLTEXT KEY ft_content (content)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='新闻数据表'
        """
        
        cursor.execute(create_table_query)
        print("✓ 新闻表创建成功")
        
        # 验证表结构
        cursor.execute(f"DESCRIBE news_{tag}")
        print("\n表结构:")
        for column in cursor.fetchall():
            # 将字节串转换为字符串
            col_name = column[0].decode('utf-8') if isinstance(column[0], bytes) else str(column[0])
            col_type = column[1].decode('utf-8') if isinstance(column[1], bytes) else str(column[1])
            col_null = column[2].decode('utf-8') if isinstance(column[2], bytes) else str(column[2])
            
            print(f"  {col_name:15} {col_type:20} {col_null}")
        
        connection.commit()
        
    except mysql.connector.Error as e:
        print(f"错误: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    tags=["fmxw","tzgg","ynxw","xyld","dss","xss","syfc","xsyx"]
    for tag in tags:
        create_news_table(tag)