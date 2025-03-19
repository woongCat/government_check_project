from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=api_key)

# GPT-4o 모델 호출
completion = client.chat.completions.create(
    model="gpt-4o", messages=[{"role": "user", "content": "Write a haiku about AI"}]
)

# 응답 출력
print(completion.choices[0].message.content)
