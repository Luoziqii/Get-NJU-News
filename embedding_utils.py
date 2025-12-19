import dashscope
from http import HTTPStatus
import logging


def get_embedding(text, api_key):
    dashscope.api_key = api_key
    # 建议截断，阿里 text-embedding-v1 限制为 2048 tokens
    safe_text = text[:1500]

    try:
        resp = dashscope.TextEmbedding.call(
            model=dashscope.TextEmbedding.Models.text_embedding_v1, input=safe_text
        )
        if resp.status_code == HTTPStatus.OK:
            return resp.output['embeddings'][0]['embedding']
        else:
            logging.error(f"Embedding API Error: {resp.message}")
            return None
    except Exception as e:
        logging.error(f"Embedding Exception: {e}")
        return None
