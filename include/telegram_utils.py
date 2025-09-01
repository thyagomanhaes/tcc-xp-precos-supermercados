import asyncio
from telegram import Bot

TELEGRAM_TOKEN = "8345402256:AAEmBvVqg_9oJuj8q4WQ7CVsHYIMvxlR09Y"
TELEGRAM_CHAT_ID = "-1002808832579"

async def send_telegram_message(token: str, chat_id: str, message: str):
    bot = Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="🚀 Teste de envio para o canal via bot!")
    print("Mensagem enviada com sucesso!")

def run():
    asyncio.run(send_telegram_message())
    