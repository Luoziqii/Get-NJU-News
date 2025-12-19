import logging
import time
from crawler_module import CrawlerModule
from keyword_module import KeywordModule  # 假设这是他人编写的模块
from vector_module import VectorModule
from email_module import EmailModule

# ================= 配置日志 =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("system_run.log", encoding='utf-8'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=== 全自动新闻爬取与推送系统 启动 ===")

    try:
        # --- 步骤 1: 爬虫抓取模块 ---
        # 功能：负责从目标网站抓取原始数据。
        # 结果：将新数据存入 MySQL，初始化字段 process_status = 0。
        logger.info("阶段 1: 启动爬虫抓取...")
        crawler = CrawlerModule()
        crawler.run()

        # --- 步骤 2: 关键词匹配模块 ---
        # 功能：扫描数据库中 process_status = 0 的记录。
        # 结果：匹配预设关键词。若命中，设置 match_type = 1。
        #      处理完成后，统一将 process_status 更新为 1。
        logger.info("阶段 2: 启动关键词匹配过滤...")
        kw_module = KeywordModule()
        kw_module.run()

        # --- 步骤 3: 向量化与语义匹配模块
        # 功能：
        #   1. 增量向量化：读取 process_status = 1 的记录，生成向量并存入 ChromaDB。
        #   2. 状态推进：将处理过的记录 process_status 更新为 2。
        #   3. 语义匹配：将用户兴趣词向量化，并在向量库中检索 Top 3 相似新闻。
        #   4. 标记匹配：对 Top 3 的记录执行位运算 match_type | 2。
        logger.info("阶段 3: 启动向量化入库与语义匹配...")
        vector_module = VectorModule()
        vector_module.run()

        # --- 步骤 4: 邮件推送模块 ---
        # 功能：从数据库筛选 process_status = 2 且 match_type > 0 的记录。
        # 结果：将命中的新闻（不论是关键词匹配还是语义匹配）汇总，发送 HTML 邮件。
        #      发送成功后，将 process_status 更新为 3 (归档状态)。
        logger.info("阶段 4: 启动邮件推送程序...")
        email_module = EmailModule()
        email_module.run()

        logger.info("=== 本轮系统运行任务全部成功完成 ===")

    except Exception as e:
        logger.error(f"!!! 系统运行中途发生崩溃: {e}", exc_info=True)


if __name__ == "__main__":
    main()
