#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
抖音直播 m3u 增量更新器

流程：
  1. 读取 sources.txt，逐条解析来源（直播间页 / 分类页 / 纯房间号）
  2. 每个来源按三级策略抓取直播间信息：
       ① HTTP 接口（分类接口带 a_bogus 参数 + room enter，最快；无需浏览器，
          每分类约 120~200 房间）
       ② 浏览器（browser_fetch.mjs，Patchright）：滚动加载 + 拦截站点签名请求，
          接口被 HTTP 风控时通常仍能拿到约 200 房间/分类
       ③ HTTP 页面 HTML 内嵌 RSC 数据（前两级都失败时兜底，每分类约 15 个置顶房间）
  3. 增量去重合并：
       - 本轮抓到的所有房间**插到列表最前面**（按来源顺序，房间号去重）
       - 旧列表中与"本轮抓到"重复的条目**删除**（让位给顶部的新条目）
       - 本轮未抓到的历史条目按原顺序保留在后面
  4. group-title 使用来源对应的类目名（从分类页动态提取，如 英雄联盟/舞蹈/音乐）

用法：
  python3 update_m3u.py             # 正常更新
  python3 update_m3u.py --dry-run   # 只打印将要做的变更，不写文件
"""
import http.cookiejar
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
M3U_PATH = os.path.join(BASE_DIR, 'douyin_live.m3u')
SOURCES_PATH = os.path.join(BASE_DIR, 'sources.txt')
BROWSER_SCRIPT = os.path.join(BASE_DIR, 'browser_fetch.mjs')

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36')

MAX_PAGES = 3            # 每个分类最多拉 3 页(15/页 ≈ 45 房间)
PAGE_SLEEP = 1.2         # 页面请求间隔，避免触发风控
SOURCE_SLEEP = 1.0       # 来源之间的间隔
BROWSER_TIMEOUT = 300    # 单个来源浏览器兜底超时(秒)

# 分类接口 URL 必须带 a_bogus 参数才会放行；服务端只校验参数存在，
# 值可复用/伪造（实测 188 位固定值 60/60 全通过，任意分类与分页通用）
A_BOGUS_PARAM = 'a' * 188
MAX_WORKERS = 4          # 来源级并发线程数（减少总耗时，实测无风控）
CATEGORY_RETRY = 3       # 分类接口遇空/风控时整体重试次数（接口偶发抖动）
RETRY_SLEEP = 2.0        # 重试间隔(秒)

# 已知分类的静态名称映射（sources.txt 默认 12 个地址；页面动态提取失败时兜底）
CATEGORY_NAMES = {
    '1010014': '英雄联盟', '1010045': '王者荣耀', '1010055': '金铲铲之战',
    '1010350': '魔兽争霸3', '1010032': '和平精英', '1011032': '三角洲行动',
    '1010092': '地下城与勇士',
    '3': '单机游戏', '1': '射击游戏', '2': '竞技游戏',
    '105': '舞蹈', '106': '文化', '107': '生活', '108': '运动',
    '102': '音乐', '104': '二次元',
}


class Session:
    """带 CookieJar 的 HTTP 会话，用于 ttwid 注册与接口请求"""

    def __init__(self):
        self.cj = http.cookiejar.CookieJar()
        self.op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cj))

    def get(self, url, referer=None, accept=None, timeout=30):
        hdr = {'User-Agent': UA, 'Accept-Language': 'zh-CN,zh;q=0.9'}
        if accept:
            hdr['Accept'] = accept
        if referer:
            hdr['Referer'] = referer
        req = urllib.request.Request(url, headers=hdr)
        r = self.op.open(req, timeout=timeout)
        return r.status, r.read(), dict(r.headers)

    def warm(self, category_path):
        """注册正式 ttwid：访问分类页拿临时 cookie，再调 union register 升级"""
        self.get('https://live.douyin.com/categorynew/' + category_path)
        body = json.dumps({
            'region': 'cn', 'aid': 6383, 'needFid': False,
            'service': 'live.douyin.com',
            'migrate_info': {'tier': '', 'from_model': 'pc'},
        }).encode()
        hdr = {'User-Agent': UA, 'Content-Type': 'application/json',
               'Accept': 'application/json'}
        req = urllib.request.Request(
            'https://ttwid.bytedance.com/ttwid/union/register/',
            data=body, headers=hdr)
        r = self.op.open(req, timeout=30)
        return r.status


def split_category(path):
    """从分类页路径推导 partition / partition_type
    例: 4_105              -> partition=105, partition_type=4
        4_103_1_2_1_1010014 -> partition=1010014, partition_type=1
    """
    seg = [s for s in path.split('_') if s]
    if len(seg) < 2:
        raise ValueError(f'无法识别分类路径: {path}')
    return seg[-1], seg[-2]


def parse_source(line):
    """把一行来源解析为 (kind, target)
    kind: 'room' / 'category'
    """
    t = line.strip()
    m = re.match(r'https?://live\.douyin\.com/categorynew/([\d_]+)', t)
    if m:
        return 'category', m.group(1)
    m = re.match(r'https?://live\.douyin\.com/(\d+)', t)
    if m:
        return 'room', m.group(1)
    if re.fullmatch(r'\d{6,15}', t):
        return 'room', t
    raise ValueError(f'无法识别的来源: {line}')


def load_sources():
    out = []
    if not os.path.exists(SOURCES_PATH):
        raise SystemExit(f'缺少来源配置文件 {SOURCES_PATH}')
    for line in open(SOURCES_PATH, encoding='utf-8'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        out.append(parse_source(line))
    if not out:
        raise SystemExit('sources.txt 中没有有效来源')
    return out


def api_params(partition, partition_type, offset, count=15):
    return {
        'aid': '6383', 'app_name': 'douyin_web', 'live_id': '1',
        'device_platform': 'web', 'language': 'zh-CN',
        'cookie_enabled': 'true', 'screen_width': '1280', 'screen_height': '720',
        'browser_language': 'zh-CN', 'browser_platform': 'Windows',
        'browser_name': 'Chrome', 'browser_version': '151.0.0.0',
        'os_name': 'Windows', 'os_version': '10',
        'count': str(count), 'offset': str(offset),
        'partition': partition, 'partition_type': partition_type, 'req_from': '2',
        'a_bogus': A_BOGUS_PARAM,
    }


def check_risk(headers, body, where):
    if 'bdturing-verify' in headers or not body:
        raise RuntimeError(f'触发风控({headers.get("bdturing-verify", "empty body")}) @ {where}')


CDN_QUALITY = ['FULL_HD1', 'HD1', 'SD1', 'SD2']


def extract_cdn_url(room):
    """从分类/enter 响应里的 stream_url 提取最高清 CDN 直链
    优先 hls_pull_url_map（m3u8），其次 flv_pull_url；转 https
    """
    su = (room or {}).get('stream_url') or {}
    hls = su.get('hls_pull_url_map') or {}
    flv = su.get('flv_pull_url') or {}
    for q in CDN_QUALITY:
        u = hls.get(q) or ''
        if u:
            return u.replace('http://', 'https://')
    for q in CDN_QUALITY:
        u = flv.get(q) or ''
        if u:
            return u.replace('http://', 'https://')
    u = su.get('hls_pull_url') or ''
    return u.replace('http://', 'https://') or None


def parse_category_item(it):
    """从分类接口单条记录取出 (rid, title, avatar, nickname)"""
    room = it.get('room') or {}
    owner = room.get('owner') or it.get('owner') or {}
    rid = str(it.get('web_rid') or room.get('web_rid') or room.get('webRid') or '').strip()
    if not (rid.isdigit() and 6 <= len(rid) <= 15):
        return None
    title = (room.get('title') or '').strip()
    nick = (owner.get('nickname') or owner.get('nick_name') or '').strip()
    avatar = ''
    # 该分类接口变体没有 owner，头像在 room.cover.url_list；逐候选取值
    for av in (it.get('avatar'), owner.get('avatar_thumb'), owner.get('avatar'), room.get('cover')):
        if isinstance(av, dict):
            ul = av.get('url_list') or []
            if ul:
                avatar = str(ul[0])
                break
        elif isinstance(av, str) and av:
            avatar = av
            break
    return {'rid': rid, 'title': title, 'avatar': avatar, 'nickname': nick,
            'url': extract_cdn_url(room)}


def http_fetch_category(sess, path):
    """① 纯 HTTP 接口拉取分类页下的直播间列表
    接口对热门分类偶发返回空/风控（抖动），整体重试数次
    """
    partition, ptype = split_category(path)
    last_err = None
    for attempt in range(CATEGORY_RETRY):
        if attempt:
            time.sleep(RETRY_SLEEP)
        rooms = []
        try:
            for page in range(MAX_PAGES):
                offset = page * 15
                url = ('https://live.douyin.com/webcast/web/partition/detail/room/v2/?'
                       + urllib.parse.urlencode(api_params(partition, ptype, offset)))
                st, body, headers = sess.get(url, referer='https://live.douyin.com/categorynew/' + path)
                check_risk(headers, body, f'分类接口 p{page}')
                j = json.loads(body)
                items = (j.get('data') or {}).get('data') or []
                for it in items:
                    r = parse_category_item(it)
                    if r:
                        rooms.append(r)
                if len(items) < 15:
                    break
                time.sleep(PAGE_SLEEP)
            if rooms:
                return rooms
            last_err = '空数据'
            print(f'  [接口] {path}: 第{attempt + 1}次尝试为空, 重试')
        except Exception as e:
            last_err = str(e)
            print(f'  [接口] {path}: 第{attempt + 1}次尝试失败({str(e)[:60]}), 重试')
    raise RuntimeError(f'分类接口重试{CATEGORY_RETRY}次仍失败: {last_err}')


def http_fetch_room(sess, rid):
    """① 纯 HTTP 接口拉取单个直播间信息（标题/主播/头像）"""
    sess.get(f'https://live.douyin.com/{rid}', referer='https://www.google.com/')
    params = {
        'aid': '6383', 'app_name': 'douyin_web', 'live_id': '1',
        'device_platform': 'web', 'language': 'zh-CN', 'enter_from': 'link_share',
        'cookie_enabled': 'true', 'screen_width': '1280', 'screen_height': '720',
        'browser_language': 'zh-CN', 'browser_platform': 'Windows',
        'browser_name': 'Chrome', 'browser_version': '151.0.0.0',
        'os_name': 'Windows', 'os_version': '10',
        'web_rid': rid, 'room_id_str': '', 'enter_source': '',
        'is_need_double_stream': 'false', 'insert_task_id': '', 'live_reason': '',
    }
    url = 'https://live.douyin.com/webcast/room/web/enter/?' + urllib.parse.urlencode(params)
    st, body, headers = sess.get(
        url, referer=f'https://live.douyin.com/{rid}',
        accept='application/json, text/plain, */*')
    check_risk(headers, body, 'enter 接口')
    j = json.loads(body)
    d0 = (j.get('data') or {}).get('data') or [None]
    if not d0 or not d0[0]:
        return []
    d = d0[0]
    user = d.get('owner') or d.get('user') or {}
    title = (d.get('title') or '').strip()
    nick = (user.get('nickname') or user.get('nick_name') or '').strip()
    avatar = ''
    av = user.get('avatar_thumb') or user.get('avatar') or {}
    if isinstance(av, dict):
        ul = av.get('url_list') or []
        if ul:
            avatar = str(ul[0])
    return [{'rid': rid, 'title': title, 'avatar': avatar, 'nickname': nick,
             'url': extract_cdn_url(d)}]


def extract_category_names(blob):
    """从 RSC 数据里的 categoryData 分类树提取 {(type, id): 类目名}
    例如 4_103_1_2_1_1010014 -> (1,'1010014') => '英雄联盟'
    """
    i = blob.find('"categoryData":')
    if i < 0:
        return {}
    j = blob.find('[', i)
    if j < 0:
        return {}
    depth = 0
    arr = None
    for k in range(j, len(blob)):
        c = blob[k]
        if c == '[':
            depth += 1
        elif c == ']':
            depth -= 1
            if depth == 0:
                try:
                    arr = json.loads(blob[j:k + 1])
                except Exception:
                    arr = None
                break
    if not arr:
        return {}
    out = {}

    def walk(nodes):
        for n in nodes:
            part = n.get('partition') or {}
            tid = str(part.get('id_str') or '')
            ty = part.get('type')
            if tid:
                out.setdefault((ty, tid), (part.get('title') or '').strip())
            walk(n.get('sub_partition') or [])

    walk(arr)
    return out


def category_group_name(path, names=None):
    """分类路径 -> group-title 名称
    优先用页面动态提取的类目名(取路径最深层)，其次静态映射，最后回退'抖音'
    """
    seg = [s for s in path.split('_') if s]
    pairs = []
    for a, b in zip(seg[::2], seg[1::2]):
        pairs.append((int(a), b))
    if not pairs:
        return '抖音'
    if names:
        for ty, pid in reversed(pairs):
            t = names.get((ty, pid))
            if t:
                return t
    return CATEGORY_NAMES.get(pairs[-1][1], '抖音')


def parse_page_html(html):
    """② 解析页面 HTML 内嵌的 RSC 数据（self.__pace_f.push 块）
    接口被风控时页面 GET 仍可用；分类页约 15 个置顶房间，直播间页单个
    返回 (rooms, category_names)
    """
    parts = []
    for m in re.finditer(r'self\.__pace_f\.push\(\[1,"', html):
        s = m.end()
        e = html.find('"])</script>', s)
        if e < 0:
            break
        try:
            parts.append(json.loads('"' + html[s:e] + '"'))
        except Exception:
            continue
    blob = ''.join(parts)
    if not blob:
        raise RuntimeError('页面中未找到 RSC 数据')

    def extract_obj(idx):
        depth = 0
        start = idx
        for i in range(idx - 1, -1, -1):
            c = blob[i]
            if c == '}':
                depth += 1
            elif c == '{':
                if depth == 0:
                    start = i
                    break
                depth -= 1
        depth = 0
        for i in range(start, len(blob)):
            c = blob[i]
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    return start, i + 1
        return None

    rooms, seen = [], set()
    for m in re.finditer(r'"web_rid":"(\d{6,15})"', blob):
        rid = m.group(1)
        if rid in seen:
            continue
        span = extract_obj(m.start())
        if not span:
            continue
        try:
            obj = json.loads(blob[slice(*span)])
        except Exception:
            continue
        rm = obj.get('room') or obj
        owner = rm.get('owner') or obj.get('owner') or obj.get('user') or {}
        title = (rm.get('title') or '').strip()
        nick = (owner.get('nickname') or owner.get('nick_name') or '').strip()
        avatar = ''
        av = owner.get('avatar_thumb') or owner.get('avatar') or {}
        if isinstance(av, dict):
            ul = av.get('url_list') or []
            if ul:
                avatar = str(ul[0])
        elif isinstance(av, str):
            avatar = av
        rooms.append({'rid': rid, 'title': title, 'avatar': avatar, 'nickname': nick,
                      'url': extract_cdn_url(rm)})
        seen.add(rid)
    names = extract_category_names(blob)
    return rooms, names


def http_fetch_page(kind, target, sess):
    """② 纯 HTTP 拉页面并解析 RSC 数据
    返回 (rooms, category_names)
    """
    url = f'https://live.douyin.com/categorynew/{target}' if kind == 'category' \
        else f'https://live.douyin.com/{target}'
    referer = 'https://www.google.com/' if kind == 'room' else None
    _st, body, _headers = sess.get(url, referer=referer)
    return parse_page_html(body.decode('utf-8', 'ignore'))


_browser_lock = threading.Lock()


def browser_fetch(kind, target):
    """③ 浏览器兜底：调用 browser_fetch.mjs（Patchright）
    返回 [{'rid', 'title', 'avatar', 'nickname'}]
    """
    url = (f'https://live.douyin.com/categorynew/{target}' if kind == 'category'
           else f'https://live.douyin.com/{target}')
    # 多线程并发时浏览器实例串行，避免多个 Chromium 抢资源
    with _browser_lock:
        r = subprocess.run(
            ['node', BROWSER_SCRIPT, url],
            capture_output=True, text=True, timeout=BROWSER_TIMEOUT)
    if r.returncode != 0:
        raise RuntimeError('浏览器兜底失败: ' + (r.stderr.strip() or r.stdout.strip())[-300:])
    rooms = json.loads(r.stdout)
    if not isinstance(rooms, list):
        raise RuntimeError('浏览器兜底返回格式错误')
    return rooms


def fetch_source(kind, target, sess):
    """分类: ①接口 -> ②浏览器(滚动) -> ③页面;   直播间: ①接口 -> ②页面 -> ③浏览器
    返回 (rooms, method, group_name)
    """
    group = category_group_name(target) if kind == 'category' else '抖音'
    # ① 接口（最快，IP 干净时房间最多）
    try:
        rooms = http_fetch_category(sess, target) if kind == 'category' else http_fetch_room(sess, target)
        if rooms:
            return rooms, '接口', group
        print(f'  [接口] {target}: 空数据, 换下一级')
    except Exception as e:
        print(f'  [接口] {target}: {e}')

    if kind == 'category':
        # ② 浏览器滚动加载：站点自身签约请求通常能绕过 HTTP 接口的 IP 风控
        try:
            rooms = browser_fetch(kind, target)
            if rooms:
                return rooms, '浏览器', group
        except Exception as e:
            print(f'  [浏览器] {target}: {e}')
        # ③ 页面静态数据兜底
        try:
            rooms, names = http_fetch_page(kind, target, sess)
            group = category_group_name(target, names)
            if rooms:
                return rooms, '页面', group
        except Exception as e:
            print(f'  [页面] {target}: {e}')
        raise RuntimeError('接口/浏览器/页面 三级均失败')
    else:
        # 直播间：页面静态数据兜底后，最后再试浏览器
        try:
            rooms, _names = http_fetch_page(kind, target, sess)
            if rooms:
                return rooms, '页面', group
        except Exception as e:
            print(f'  [页面] {target}: {e}')
        rooms = browser_fetch(kind, target)
        return rooms, '浏览器', group


def read_existing_m3u():
    """解析现有 m3u：保留文件头 + 按顺序去重后的条目列表
    返回 (header, [(rid, extinf_line, url_line)])，重复 rid 只保留最先出现的一条
    """
    header = '#EXTM3U\n'
    entries = []
    seen = set()
    if os.path.exists(M3U_PATH):
        lines = open(M3U_PATH, encoding='utf-8').read().splitlines()
    else:
        lines = []
    i = 0
    while i < len(lines):
        l = lines[i]
        if i == 0 and l.startswith('#EXTM3U'):
            header = l + '\n'
            i += 1
            continue
        if l.startswith('#EXTINF') and i + 1 < len(lines) and lines[i + 1].startswith('http'):
            m = re.search(r'tvg-id="(\d+)"', l)
            if not m:
                m = re.search(r'/room/(\d+)', lines[i + 1])
            if m:
                rid = m.group(1)
                if rid not in seen:
                    seen.add(rid)
                    entries.append((rid, lines[i], lines[i + 1]))
            i += 2
            continue
        i += 1
    return header, entries, seen


def clean_text(s):
    return re.sub(r'["\r\n]', '', s or '').replace(',', '，').strip()


def render_entry(room, group_name='抖音'):
    nick = clean_text(room.get('nickname') or '')
    title = clean_text(room.get('title') or '')
    if nick and title:
        t = f'{nick}-{title}'
    elif nick:
        t = nick
    elif title:
        t = title
    else:
        t = room['rid']
    logo = room.get('avatar') or ''
    if not logo.startswith('http'):
        logo = ''
    g = clean_text(group_name) or '抖音'
    url = room.get('url') or f'https://douyin-m3u8.pages.dev/room/{room["rid"]}'
    return (f'#EXTINF:-1 tvg-logo="{logo}" group-title="{g}" tvg-id="{room["rid"]}", {t}',
            url)


def _fetch_one(args):
    """单个来源抓取任务（线程池 worker 用）
    返回 (kind, target, rooms|None, method, group|None, err|None)
    """
    kind, target, sess, http_ok = args
    try:
        if http_ok:
            rooms, method, group = fetch_source(kind, target, sess)
        else:
            # ttwid 都拿不到时，跳过硬编码的接口层：分类先浏览器，直播间先页面
            if kind == 'category':
                rooms = browser_fetch(kind, target)
                method = '浏览器'
                group = category_group_name(target)
            else:
                rooms, _names = http_fetch_page(kind, target, sess)
                method = '页面'
                group = '抖音'
        return kind, target, rooms, method, group, None
    except Exception as e:
        return kind, target, None, None, None, str(e)


def main():
    dry_run = '--dry-run' in sys.argv
    sources = load_sources()
    print(f'共 {len(sources)} 个来源')

    sess = Session()
    http_ok = True
    try:
        first = sources[0][1] if sources[0][0] == 'category' else '4_105'
        st = sess.warm(first)
        print(f'ttwid 初始化完成, status={st}')
    except Exception as e:
        http_ok = False
        print(f'⚠ ttwid 初始化失败({e})，跳过 ①接口 层级')

    # 来源级并发：每个 worker 用独立 Session（复制 master 的 ttwid cookie），
    # 避免共享 CookieJar 的线程竞争；取回结果后仍按 sources 顺序合并
    tasks = []
    for kind, target in sources:
        shard = Session()
        for c in sess.cj:
            shard.cj.set_cookie(c)
        tasks.append((kind, target, shard, http_ok))

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for r in ex.map(_fetch_one, tasks):
            results.append(r)

    new_rooms = []        # 本轮抓到的所有房间（按来源顺序，房间号去重）
    seen_new = set()
    group_of = {}         # rid -> group-title
    failed = []
    counts = {'接口': 0, '浏览器': 0, '页面': 0}
    for kind, target, rooms, method, group, err in results:
        if err is not None or not rooms:
            failed.append(target)
            print(f'  [全部失败] {target}: {err or "空数据"}')
            continue
        counts[method] += 1
        print(f'  [{method}] {target}: {len(rooms)} 个, group="{group}"')
        for r in rooms:
            if r['rid'] not in seen_new:
                seen_new.add(r['rid'])
                new_rooms.append(r)
                group_of[r['rid']] = group

    # 🔥 每天凌晨 0 点清空一次，其他时间增量更新
    import datetime
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    if not hasattr(main, '_last_date') or main._last_date != today:
        main._last_date = today
        if os.path.exists(M3U_PATH):
            with open(M3U_PATH, 'w', encoding='utf-8') as f:
                f.write('#EXTM3U\n')
            print('📅 新的一天，已清空旧文件，重新生成')
        else:
            print('📅 新的一天，文件不存在，直接生成')

    header, old_entries, old_seen = read_existing_m3u()
    top_rids = {r['rid'] for r in new_rooms}
    added = [r for r in new_rooms if r['rid'] not in old_seen]      # 真正新增
    refreshed = [r for r in new_rooms if r['rid'] in old_seen]      # 已存在、本次置顶刷新
    removed = [e for e in old_entries if e[0] in top_rids]          # 被删除的旧重复条目
    kept = [e for e in old_entries if e[0] not in top_rids]         # 本轮未抓到、保留

    print(f'\n抓取统计: {counts}，失败来源: {len(failed)} 个')

    if dry_run:
        print(f'[DRY-RUN] 新增 {len(added)} 条、置顶刷新 {len(refreshed)} 条、'
              f'删除旧重复 {len(removed)} 条、保留历史 {len(kept)} 条')
        for r in added[:30]:
            t = clean_text(r['title']) or clean_text(r['nickname']) or r['rid']
            print(f'  + {r["rid"]}  [{group_of[r["rid"]]}] {t[:36]}')
        if len(added) > 30:
            print(f'  ... 等共 {len(added)} 条')
        return 0

    if not new_rooms:
        print(f'没有抓到任何房间（保留现有 {len(old_entries)} 条不变）')
        return 0 if not failed else 1

    lines = []
    # 本轮抓到的全部放最前面（已去重）
    for r in new_rooms:
        extinf, url = render_entry(r, group_of[r['rid']])
        lines.append(extinf)
        lines.append(url)
    # 未抓到的历史条目按原顺序保留
    for _rid, extinf, url in kept:
        lines.append(extinf)
        lines.append(url)
    with open(M3U_PATH, 'w', encoding='utf-8') as f:
        f.write(header + '\n'.join(lines) + '\n')

    print(f'完成: 新增 {len(added)} 条, 置顶刷新 {len(refreshed)} 条, '
          f'删除旧重复 {len(removed)} 条, 保留历史 {len(kept)} 条, 合计 {len(lines) // 2} 条')
    for r in added[:10]:
        t = clean_text(r['title']) or clean_text(r['nickname']) or r['rid']
        print(f'  + {r["rid"]}  [{group_of[r["rid"]]}] {t[:36]}')
    if len(added) > 10:
        print(f'  ... 其余 {len(added) - 10} 条略')
    # 部分来源失败（如分类接口临时抖动）不视为失败：m3u 已成功更新
    # 仅当完全没有抓到任何房间（上面 not new_rooms 分支）才返回 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())