from aiohttp import web
from sanic import Sanic
from sanic.response import html
from sanic.response import json as sjson
import socketio
import json, time, random
import os, time, asyncio
from bulk_fetch import fetch
import aiohttp


class AFileStream(object):
    def __init__(self, name, delay=1):
        self.name = name
        self.delay = delay

    async def read_stream(self):
        with open(self.name, "r") as current:
            current.seek(0)      # Go to the end of the file
            while True:
                 content = await self.parse(current.read().strip())
                 if not content:
                     time.sleep(self.delay)    # Sleep briefly
                     continue
                 return content

    async def parse(self, stream):
        t = time.time()
        output = { "time": t }
        lines = stream.split("\n")
        for line in lines:
            key, val = line.split(":")
            output[key.strip()] = val.strip()
        return output

    def __await__(self):
        return self.read_stream().__await__()



sio = socketio.AsyncServer(async_mode='sanic')
# app = web.Application()
# sio.attach(app)
app = Sanic()
sio.attach(app)
fs = AFileStream("/proc/meminfo")

# async def index(request):
#     with open('latency.html') as f:
#         return web.Response(text=f.read(), content_type='text/html')

@app.route('/get/')
async def index(request):
    async with aiohttp.ClientSession() as sess:
        data = asyncio.gather(*[fetch(sess, 'http://httpbin.org/get?id={}'.format(link)) for link in range(10000)])
        return sjson(await data)


@app.route('/')
def index(request):
    with open('latency.html') as f:
        return html(f.read())

@sio.on('ping_from_client')
async def ping(sid):
    data = await fs
    await sio.emit('pong_from_server', data, room=sid)

# istek atcak

# app.router.add_get('/', index)

if __name__ == '__main__':
    # web.run_app(app)
    app.run()
