import mysql.connector
import chromadb
import configparser
import logging
import json
from embedding_utils import get_embedding

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VectorModule:
    def __init__(self, config_path='config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf-8')

        # 1. 配置项读取
        self.db_params = {
            "host": self.config.get("DATABASE", "HOST"),
            "user": self.config.get("DATABASE", "USER"),
            "password": self.config.get("DATABASE", "PASSWORD"),
            "database": self.config.get("DATABASE", "DATABASE_NAME"),
        }
        self.api_key = self.config.get("VECTOR", "DASH_SCOPE_KEY")
        self.user_interest = self.config.get("VECTOR", "USER_QUERY")  # 用户兴趣描述
        self.threshold = 0.5  # 相似度距离阈值，越小越近

        # 2. 初始化 ChromaDB (本地持久化)
        self.chroma_client = chromadb.PersistentClient(path="./chroma_db")
        self.collection = self.chroma_client.get_or_create_collection(
            name="news_collection"
        )

    def _get_connection(self):
        return mysql.connector.connect(**self.db_params)

    def process_embeddings(self):
        """
        任务 A: 增量向量化
        读取 process_status = 1 的记录 -> 生成向量 -> 存入 ChromaDB -> 更新 status 为 2
        """
        conn = self._get_connection()
        cursor = conn.cursor(dictionary=True)

        # 只处理前置工序（关键词匹配）完成的数据
        cursor.execute(
            "SELECT id, title, content FROM news_all WHERE process_status = 1"
        )
        records = cursor.fetchall()

        if not records:
            logger.info("没有发现 process_status=1 的新数据，跳过向量化步骤。")
            return

        logger.info(f"正在为 {len(records)} 条记录生成向量...")
        for row in records:
            # 组合标题与正文前300字作为向量输入
            input_text = f"标题: {row['title']} 内容: {row['content'][:300]}"
            vector = get_embedding(input_text, self.api_key)

            if vector:
                # 1. 存入向量库
                self.collection.upsert(
                    ids=[str(row['id'])],
                    embeddings=[vector],
                    documents=[row['title']],
                    metadatas=[{"db_id": row['id']}],
                )
                # 2. 更新 MySQL 状态为 2 (向量处理完成)
                cursor.execute(
                    "UPDATE news_all SET process_status = 2 WHERE id = %s", (row['id'],)
                )

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("增量向量化入库完成。")

    def match_interests(self):
        """
        任务 B: 语义搜索匹配
        基于用户兴趣查询 Top 3 -> 更新 match_type (位运算 | 2)
        """
        if not self.user_interest:
            logger.warning("未配置用户兴趣词 (USER_QUERY)，跳过语义匹配。")
            return

        # 1. 将查询词转化为向量
        query_vec = get_embedding(self.user_interest, self.api_key)
        if not query_vec:
            return

        # 2. 从 ChromaDB 检索 Top 3
        results = self.collection.query(
            query_embeddings=[query_vec], n_results=3, include=["distances"]
        )

        matched_ids = results['ids'][0]
        distances = results['distances'][0]

        # 3. 筛选符合距离阈值的结果
        valid_db_ids = [
            m_id for m_id, dist in zip(matched_ids, distances) if dist <= self.threshold
        ]

        if not valid_db_ids:
            logger.info("未发现符合相似度阈值的语义匹配内容。")
            return

        # 4. 更新 MySQL match_type (位掩码 2: 二进制 010)
        conn = self._get_connection()
        cursor = conn.cursor()

        # 逻辑：match_type = match_type | 2 (保留原有的关键词匹配状态 1)
        placeholders = ','.join(['%s'] * len(valid_db_ids))
        update_sql = f"UPDATE news_all SET match_type = match_type | 2 WHERE id IN ({placeholders})"

        try:
            cursor.execute(update_sql, tuple(valid_db_ids))
            conn.commit()
            logger.info(f"语义匹配成功！更新 ID 列表: {valid_db_ids}")
        except Exception as e:
            logger.error(f"更新匹配状态失败: {e}")
        finally:
            cursor.close()
            conn.close()

    def run(self):
        """主入口"""
        # 第一步：把 status 1 变成 2
        self.process_embeddings()
        # 第二步：在 status 2 的数据中标记 match_type
        self.match_interests()


if __name__ == "__main__":
    module = VectorModule()
    module.run()
