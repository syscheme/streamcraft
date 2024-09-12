import asyncio
from pyppeteer import launch # pip3 install pyppeteer
# on ubuntu: apt install ffmpeg 
# chinese fonts if needed: apt install fontconfig fonts-arphic-gbsn00lp
import subprocess
import os, time
from datetime import datetime

from io import BytesIO
from PIL import Image

async def capture_screenshot(urls, ffmpeg_process, width=1920, height=1080):
    # 启动浏览器
    browser = await launch({
        'handleSIGINT': False,
        'handleSIGTERM': False,
        'handleSIGHUP': False,  # 
        'headless': True,
        'dumpio': True,
        'autoClose': False,  # 避免长时间运行 内存泄漏
        'args': [
            '--no-sandbox',  # 禁止沙箱模式
            '--no-default-browser-check',  # 不检查默认浏览器
            '--disable-extensions',
            '--hide-scrollbars',
            '--disable-bundled-ppapi-flash',
            '--mute-audio',
            '--disable-setuid-sandbox',
            '--disable-gpu',
            "--window-size=1000,900",
            "--disable-infobars"  # 禁止提示 浏览器被驱动的提示信息
        ],
    })

    if isinstance(urls, str): urls = urls.split(',')
    pages = []
    for url in urls :
        page = await browser.newPage()

        # 设置视口大小
        await page.setViewport({'width': width, 'height': height})

        # 导航到指定 URLipa
        await page.goto(url)

        pages.append(page)

    # 捕获屏幕截图并通过管道传给 FFmpeg
    tm_next = time.time()
    while True:
        fn_png = f'/data/page_{datetime.now().strftime("%H%M%S.%f")}.png'

        merged = None
        for page in pages:
            # await page.screenshot({'path': fn_png, 'type':'png', 'fullPage': True, 'omitBackground':True})
            screenshot = await page.screenshot({'type':'png', 'fullPage': True, 'omitBackground':True})
            screenshot = Image.open(BytesIO(screenshot))
            if not merged :
                merged = screenshot
            else :
                alpha = screenshot.split()[-1]
                # non_transparent_pixels = alpha.getchannel('1')
                # merged.paste(screenshot, (0,0), mask=non_transparent_pixels)
                merged.paste(screenshot, None, mask=alpha) # default align at left-top, mask=alpha

            merged.save(fn_png, format='PNG')

        # screenshot = await page.screenshot({'type': 'png'})
        # ffmpeg_process.stdin.write(screenshot)
        tm_now = time.time()
        tm_next += 0.5
        next_sleep = tm_next - time.time()
        if next_sleep >0 : time.sleep(next_sleep)

    # 关闭浏览器
    await browser.close()

# =============================================================
if __name__ == '__main__':
    # url = 'http://192.168.82.18:8501/under_construction'
    # url = 'file:///data/CG_graph/C1_1080i50-Guajiao01.swf'
    url = 'file:///data/CG_graph/NE_Logo2/' # not work
    url = 'file:///data/CG_graph/NE_Logo2/index.html' # works
    url = 'file:///data/CG_graph/NE_Logo/index.html'
    # url = 'file:///data/CG_graph/NW_Clock/index.html'
    
    # url = 'file:///data/CG_graph/NE_Logo2/images/Logo_atlas_.png'
    # url = 'http://news.sina.com.cn/'
    url = 'file:///data/CG_graph/NE_Logo/index.html,file:///data/CG_graph/NW_Clock/index.html'
    width, height =1920, 1080
    # width, height =3840, 2160
    # width, height =480, 240

    # 启动 FFmpeg 进程
    ffmpeg_command = f'ffmpeg -f rawvideo -pix_fmt rgba -s {width}x{height} -r 5 -i - -c:v libx264 -preset ultrafast -crf 23'
    ffmpeg_command += ' -f mp4 -' # output
    # ffmpeg_command += ' -f mpegts -omit_video_pes_length 1 udp://192.168.82.123:8000' # output
    # ffmpeg_command += ' -f mp4 /data/aaa.mp4' # output

    ffmpeg_process = subprocess.Popen(ffmpeg_command.split(' '), stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    asyncio.run(capture_screenshot(url, ffmpeg_process, width, height))

    # 关闭 FFmpeg 进程
    ffmpeg_process.stdin.close()
    ffmpeg_process.terminate()
