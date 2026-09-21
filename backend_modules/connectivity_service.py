"""Admin-only, fixed-target HTTP connectivity probes. Never returns credentials."""
import os
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

_SLOTS = threading.BoundedSemaphore(4)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def targets(media, notifications, moviepilot, proxy):
    rows = []
    def add(key, name, url, icon, group="common", route=proxy, headers=None, auth=False):
        try:
            parsed = urllib.parse.urlsplit(url)
        except ValueError:
            parsed = urllib.parse.urlsplit("")
        if parsed.hostname and urllib.request.proxy_bypass(parsed.hostname):
            route = ""
        valid = parsed.scheme in {"http", "https"} and bool(parsed.hostname) and not parsed.username and not parsed.password
        rows.append(dict(id=key, name=name, domain=parsed.hostname or "未配置", icon=icon,
                         group=group, configured=valid, route="代理" if route else "直连",
                         url=url if valid else "", proxy=route, headers=headers or {}, auth=auth))
    token = str(media.get("tmdbToken") or "").strip()
    add("tmdb", "TMDB API", "https://api.themoviedb.org/3/configuration", "TMDB",
        headers={"Authorization": "Bearer " + token} if token else {}, auth=bool(token))
    add("tmdb-image", "TMDB 海报", "https://image.tmdb.org/t/p/w92/8Gxv8gSFCU0XGDykEGv7zR1n2ua.jpg", "TMDB")
    telegram = (notifications.get("channels") or {}).get("telegram") or {}
    tg_token = str(telegram.get("botToken") or "").strip()
    tg_proxy = str(telegram.get("proxyUrl") or os.environ.get("TG_PROXY_URL") or "").strip()
    # Match Telegram's independent proxy, then system environment proxy.
    tg_proxy = tg_proxy or urllib.request.getproxies().get("https", "")
    add("telegram", "Telegram", "https://api.telegram.org/" + ("bot" + urllib.parse.quote(tg_token, safe=":") + "/getMe" if tg_token else ""), "TG", route=tg_proxy, auth=bool(tg_token))
    base = str(media.get("serverUrl") or "").rstrip("/")
    key = str(media.get("apiKey") or "")
    add("media", "媒体服务器", base + "/System/Info" if base else "", "媒体", route="", headers={"X-Emby-Token": key} if key else {}, auth=bool(key))
    mp = str(moviepilot.get("baseUrl") or "").rstrip("/")
    add("moviepilot", "MoviePilot", mp, "MP", route="")
    add("wecom", "企业微信", "https://qyapi.weixin.qq.com/", "企微")
    for key, name, url, icon in [
        ("tmdb-web", "TMDB 网站", "https://www.themoviedb.org/", "TMDB"),
        ("github", "GitHub", "https://github.com/", "GH"),
        ("github-api", "GitHub API", "https://api.github.com/", "GH"),
        ("github-raw", "GitHub Raw", "https://raw.githubusercontent.com/", "GH"),
        ("github-download", "GitHub 下载", "https://codeload.github.com/", "GH"),
        ("pypi", "PyPI", "https://pypi.org/", "Py")]:
        add(key, name, url, icon, "extended")
    return rows


def public_target(row):
    return {key: row[key] for key in ("id", "name", "domain", "icon", "group", "configured", "route")}


def probe(row, timeout=10):
    result = dict(public_target(row), status="not_configured", message="请先保存服务地址", elapsedMs=None)
    if not row["configured"]:
        return result
    if not _SLOTS.acquire(blocking=False):
        return dict(result, status="busy", message="检测繁忙，请稍后重试")
    done = threading.Event()
    started = time.monotonic()
    answer = {}
    def worker():
        code = None
        status, message = "error", "连接失败，请检查网络或代理"
        try:
            proxies = {"http": row["proxy"], "https": row["proxy"]} if row["proxy"] else {}
            opener = urllib.request.build_opener(urllib.request.ProxyHandler(proxies), NoRedirect())
            req = urllib.request.Request(row["url"], headers={"User-Agent": "VistaMirror-Connectivity/1.0", **row["headers"]})
            with opener.open(req, timeout=timeout) as response:
                code = response.status
            status, message = "connected", "可达" + (" · 鉴权通过" if row["auth"] else " · 未验证鉴权")
        except urllib.error.HTTPError as exc:
            code = exc.code
            try:
                exc.close()
            except Exception:
                pass
            if row["auth"] and code in {401, 403}:
                status, message = "auth_error", "网络可达，凭据无效或权限不足"
            elif 300 <= code < 400:
                status, message = "connected", "可达 · 返回重定向（未跟随）"
            else:
                status, message = "http_error", "网络可达，HTTP %s（未验证业务可用性）" % code
        except Exception as exc:
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, (TimeoutError, socket.timeout)):
                status, message = "timeout", "连接超时"
            elif isinstance(reason, socket.gaierror):
                status, message = "error", "域名解析失败"
            elif isinstance(reason, ssl.SSLError):
                status, message = "error", "TLS 证书或握手失败"
            else:
                status, message = "error", "连接失败，请检查网络或代理"
        finally:
            answer.update(status=status, message=message, httpStatus=code, elapsedMs=round((time.monotonic()-started)*1000))
            _SLOTS.release()
            done.set()
    threading.Thread(target=worker, daemon=True).start()
    if not done.wait(timeout + .2):
        return dict(result, status="timeout", message="检测超时（包含 DNS 与连接等待）", elapsedMs=round((time.monotonic()-started)*1000))
    return dict(result, **answer)
