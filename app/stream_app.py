import faust

app = faust.App(
    'message-processor',
    broker='kafka://kafka:9092',
    value_serializer='json',
)

@app.agent(app.topic('messages'))
async def process(stream):
    async for msg in stream:
        print(f"Received: {msg}")

if __name__ == '__main__':
    app.main()