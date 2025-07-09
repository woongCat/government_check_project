from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from sentence_transformers import SentenceTransformer

from modules.utils.db_connections import get_postgres_connection

conn = get_postgres_connection()

cursor = conn.cursor()
cursor.execute("SELECT id, text, speaker_id, date, title FROM speeches")
rows = cursor.fetchall()


model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
texts = [row[1] for row in rows]
embeddings = model.encode(texts)

client = QdrantClient(host="localhost", port=6333)

points = [
    PointStruct(
        id=row[0],
        vector=embedding,
        payload={
            "text": row[1],
            "speaker_id": row[2],  # 예: speaker_id='홍길동'
            "date": row[3],  # 예: 회의 일자
            "title": row[4],  # 예: 회의 제목
        },
    )
    for row, embedding in zip(rows, embeddings)
]

client.upsert(collection_name="speeches", points=points)
