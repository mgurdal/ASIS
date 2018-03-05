import asyncio
import aiohttp
import random


async def fetch(sess, url):
    async with sess.get(url) as response:
        data = await response.json()
        data['image'] = "https://picsum.photos/200/300/?image={}".format(int(random.random()*600))
        return data

async def bind(sem, sess,  url):
    async with sem:
        await fetch(sess, url)

async def collect(links):
    tasks = []
    sem = asyncio.Semaphore(1000)
    async with aiohttp.ClientSession() as sess:
        for link in links:
            resp = bind(sem, sess, link) # 3 - 1 - 5
            tasks.append(resp)
        responses = asyncio.gather(*tasks)
        data = await responses



if __name__ == '__main__':
    import time
    start = time.time()
    print("Started at: ", start)
    target_links = ["http://httpbin.org/get" for x in range(1000)]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collect(target_links))
    print("Finished: ", time.time() - start)
