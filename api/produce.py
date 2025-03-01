from fastapi import APIRouter, Request
from starlette.responses import JSONResponse

from config import producer, delivery_report

produce_router = APIRouter()


@produce_router.post("/{topic_name}/send")
async def create_favourites(topic_name: str, request: Request):
    """
    Отправляет сообщение в топик.

    **body:**
    - `message` (`str`): сообщение которое должно упасть в переданный топик.

    """
    json_text = await request.json()
    if json_text is None:
        return JSONResponse(
            status_code=404,
            content={"message": f"Configuration was not sent"},
        )

    producer.produce(topic_name, key=None, value=json_text.get('message'), callback=delivery_report)
    producer.flush()
    return {"status": "Message sent", "message": json_text.get('message')}
