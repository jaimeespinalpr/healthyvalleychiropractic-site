#!/usr/bin/env python3
"""Static mirror crawler for healthyvalleychiropractic.com.

Downloads HTML pages and static assets, rewrites links to local paths,
and builds a GitHub Pages friendly static mirror.
"""

from __future__ import annotations

import os
import posixpath
import re
import sys
import time
import hashlib
from collections import deque
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import parse_qsl, quote, unquote, urljoin, urlparse, urlunparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

START_URL = "https://www.healthyvalleychiropractic.com/"
PRIMARY_HOST = "www.healthyvalleychiropractic.com"
ALLOWED_HOSTS = {PRIMARY_HOST, "healthyvalleychiropractic.com"}
OUTPUT_DIR = "site"

MAX_PAGES = 2000
MAX_ASSETS = 20000
REQUEST_TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36 MirrorBot/1.0"
)

HTML_EXTENSIONS = {"", ".html", ".htm", ".php", ".asp", ".aspx", ".jsp"}
ASSET_EXTENSIONS = {
    ".css",
    ".js",
    ".mjs",
    ".json",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".bmp",
    ".avif",
    ".tif",
    ".tiff",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
    ".eot",
    ".pdf",
    ".mp4",
    ".webm",
    ".ogg",
    ".mp3",
    ".wav",
    ".txt",
    ".xml",
}

SKIP_SCHEMES = ("mailto:", "tel:", "javascript:", "data:", "#")

URL_PATTERN = re.compile(r"url\((.*?)\)", re.IGNORECASE)
IMPORT_PATTERN = re.compile(r"@import\s+(?:url\()?['\"]?([^'\"\)\s]+)", re.IGNORECASE)


@dataclass(frozen=True)
class UrlTarget:
    url: str
    is_page: bool


def clean_url(url: str, base_url: str | None = None) -> str | None:
    if not url:
        return None
    raw = url.strip()
    if not raw or raw.startswith(SKIP_SCHEMES):
        return None
    absolute = urljoin(base_url or START_URL, raw)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return None
    # Drop fragment; keep query because some resources use versioning.
    parsed = parsed._replace(fragment="")
    return urlunparse(parsed)


def is_allowed_host(host: str) -> bool:
    return (host or "").lower() in ALLOWED_HOSTS


def is_page_url(url: str) -> bool:
    p = urlparse(url)
    path = p.path or "/"
    ext = posixpath.splitext(path)[1].lower()
    if ext in ASSET_EXTENSIONS:
        return False
    if ext in HTML_EXTENSIONS:
        return True
    return not ext


def sanitize_query(query: str) -> str:
    if not query:
        return ""
    pairs = parse_qsl(query, keep_blank_values=True)
    flat = "&".join(f"{k}={v}" for k, v in pairs) or query
    digest = hashlib.sha1(flat.encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"__q_{digest}"


def shorten_segment(seg: str, max_len: int = 100) -> str:
    if len(seg) <= max_len:
        return seg
    digest = hashlib.sha1(seg.encode("utf-8", errors="ignore")).hexdigest()[:10]
    return f"{seg[:40]}__{digest}"


def url_to_local_path(url: str, is_page: bool, external_prefix: str = "_external") -> str:
    p = urlparse(url)
    host = (p.netloc or "").lower()
    path = unquote(p.path or "/")
    query_marker = sanitize_query(p.query)

    if is_allowed_host(host):
        root = ""
    else:
        root = posixpath.join(external_prefix, host)

    if path.endswith("/"):
        local = posixpath.join(path, "index.html" if is_page else "index")
    else:
        ext = posixpath.splitext(path)[1]
        if is_page and ext in {"", ".php", ".asp", ".aspx", ".jsp"}:
            local = f"{path}/index.html" if ext == "" else f"{path}.html"
        else:
            local = path

    if query_marker:
        base, ext = posixpath.splitext(local)
        if not ext:
            ext = ".html" if is_page else ""
        local = f"{base}.{query_marker}{ext}"

    local = local.lstrip("/")
    if not local:
        local = "index.html" if is_page else "index"

    parts = [shorten_segment(part) for part in local.split("/") if part]
    local = "/".join(parts)
    if len(local) > 220:
        base, ext = posixpath.splitext(local)
        digest = hashlib.sha1(local.encode("utf-8", errors="ignore")).hexdigest()[:16]
        local = f"longpath/{digest}{ext or '.bin'}"

    return posixpath.join(root, local).replace("\\", "/")


def rel_link(from_file: str, to_file: str) -> str:
    from_dir = posixpath.dirname(from_file) or "."
    rel = posixpath.relpath(to_file, from_dir).replace("\\", "/")
    return rel


def ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def split_srcset(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def join_srcset(parts: Iterable[str]) -> str:
    return ", ".join(parts)


def rewrite_css_urls(
    css_text: str,
    css_url: str,
    css_local_path: str,
    enqueue: callable,
) -> str:
    def replace_url(match: re.Match[str]) -> str:
        raw = match.group(1).strip().strip("'\"")
        normalized = clean_url(raw, css_url)
        if not normalized:
            return match.group(0)
        target = enqueue(normalized, is_page=False)
        if not target:
            return match.group(0)
        return f"url('{rel_link(css_local_path, target)}')"

    css_text = URL_PATTERN.sub(replace_url, css_text)

    def replace_import(match: re.Match[str]) -> str:
        raw = match.group(1).strip().strip("'\"")
        normalized = clean_url(raw, css_url)
        if not normalized:
            return match.group(0)
        target = enqueue(normalized, is_page=False)
        if not target:
            return match.group(0)
        return f"@import url('{rel_link(css_local_path, target)}')"

    return IMPORT_PATTERN.sub(replace_import, css_text)


class Mirror:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=2)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.page_queue: deque[str] = deque()
        self.asset_queue: deque[str] = deque()
        self.seen_pages: set[str] = set()
        self.seen_assets: set[str] = set()

        self.downloaded_pages = 0
        self.downloaded_assets = 0
        self.failed: list[tuple[str, str]] = []

    def enqueue(self, url: str, is_page: bool) -> str | None:
        normalized = clean_url(url)
        if not normalized:
            return None
        host = (urlparse(normalized).netloc or "").lower()
        if is_page and not is_allowed_host(host):
            return None

        local_path = url_to_local_path(normalized, is_page=is_page)
        if is_page:
            if normalized not in self.seen_pages and len(self.seen_pages) < MAX_PAGES:
                self.seen_pages.add(normalized)
                self.page_queue.append(normalized)
        else:
            if normalized not in self.seen_assets and len(self.seen_assets) < MAX_ASSETS:
                self.seen_assets.add(normalized)
                self.asset_queue.append(normalized)
        return local_path

    def fetch(self, url: str) -> requests.Response | None:
        try:
            r = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                self.failed.append((url, f"HTTP {r.status_code}"))
                return None
            return r
        except requests.RequestException as exc:
            self.failed.append((url, str(exc)))
            return None

    def load_sitemap_urls(self) -> list[str]:
        urls: set[str] = {START_URL}
        candidates = [
            "https://www.healthyvalleychiropractic.com/sitemap_index.xml",
            "https://www.healthyvalleychiropractic.com/sitemap.xml",
        ]
        for sitemap_url in candidates:
            resp = self.fetch(sitemap_url)
            if not resp:
                continue
            try:
                root = ET.fromstring(resp.content)
            except ET.ParseError:
                continue
            ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
            tag = root.tag.lower()
            if tag.endswith("sitemapindex"):
                for loc in root.findall(f".//{ns}loc"):
                    if loc.text:
                        child = self.fetch(loc.text.strip())
                        if not child:
                            continue
                        try:
                            croot = ET.fromstring(child.content)
                        except ET.ParseError:
                            continue
                        for uloc in croot.findall(f".//{ns}loc"):
                            if uloc.text:
                                urls.add(uloc.text.strip())
            elif tag.endswith("urlset"):
                for loc in root.findall(f".//{ns}loc"):
                    if loc.text:
                        urls.add(loc.text.strip())
        return sorted(urls)

    def process_html(self, url: str, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        current_local = url_to_local_path(url, is_page=True)

        # Remove any base tag from source site and set none for local linking.
        for base_tag in soup.find_all("base"):
            base_tag.decompose()

        def rewrite_attr(tag, attr: str, is_srcset: bool = False) -> None:
            if attr not in tag.attrs:
                return
            raw = tag.get(attr)
            if not raw:
                return

            if is_srcset:
                rewritten_parts: list[str] = []
                for part in split_srcset(raw):
                    seg = part.split()
                    if not seg:
                        continue
                    src = seg[0]
                    normalized = clean_url(src, url)
                    if not normalized:
                        rewritten_parts.append(part)
                        continue
                    host = (urlparse(normalized).netloc or "").lower()
                    target_is_page = is_allowed_host(host) and is_page_url(normalized)
                    local = self.enqueue(normalized, is_page=target_is_page)
                    if local:
                        seg[0] = rel_link(current_local, local)
                        rewritten_parts.append(" ".join(seg))
                    else:
                        rewritten_parts.append(part)
                tag[attr] = join_srcset(rewritten_parts)
                return

            normalized = clean_url(str(raw), url)
            if not normalized:
                return
            host = (urlparse(normalized).netloc or "").lower()
            target_is_page = is_allowed_host(host) and is_page_url(normalized)
            local = self.enqueue(normalized, is_page=target_is_page)
            if local:
                tag[attr] = rel_link(current_local, local)

        for tag in soup.find_all(True):
            rewrite_attr(tag, "href")
            rewrite_attr(tag, "src")
            rewrite_attr(tag, "data-src")
            rewrite_attr(tag, "data-lazy-src")
            rewrite_attr(tag, "poster")
            rewrite_attr(tag, "srcset", is_srcset=True)
            rewrite_attr(tag, "data-srcset", is_srcset=True)

            style = tag.get("style")
            if style and "url(" in style:
                tag["style"] = rewrite_css_urls(
                    style,
                    css_url=url,
                    css_local_path=current_local,
                    enqueue=self.enqueue,
                )

        for style_tag in soup.find_all("style"):
            if style_tag.string and "url(" in style_tag.string:
                style_tag.string.replace_with(
                    rewrite_css_urls(
                        style_tag.string,
                        css_url=url,
                        css_local_path=current_local,
                        enqueue=self.enqueue,
                    )
                )
        return str(soup)

    def write_file(self, local_path: str, data: bytes) -> None:
        file_path = os.path.join(OUTPUT_DIR, local_path.replace("/", os.sep))
        ensure_parent(file_path)
        with open(file_path, "wb") as f:
            f.write(data)

    def crawl(self) -> None:
        if os.path.isdir(OUTPUT_DIR):
            # Preserve .git only outside output; safe full refresh of mirror files.
            for root, dirs, files in os.walk(OUTPUT_DIR, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        for url in self.load_sitemap_urls():
            normalized = clean_url(url)
            if not normalized:
                continue
            if is_allowed_host((urlparse(normalized).netloc or "").lower()):
                self.enqueue(normalized, is_page=is_page_url(normalized))

        self.enqueue(START_URL, is_page=True)

        while self.page_queue:
            page_url = self.page_queue.popleft()
            resp = self.fetch(page_url)
            if not resp:
                continue
            content_type = (resp.headers.get("Content-Type") or "").lower()
            local_path = url_to_local_path(page_url, is_page=True)
            if "text/html" in content_type or is_page_url(page_url):
                html = resp.text
                html = self.process_html(page_url, html)
                self.write_file(local_path, html.encode("utf-8"))
                self.downloaded_pages += 1
            else:
                self.write_file(url_to_local_path(page_url, is_page=False), resp.content)
                self.downloaded_assets += 1

            if self.downloaded_pages % 25 == 0:
                print(f"[pages] {self.downloaded_pages} done, assets queued: {len(self.asset_queue)}")

        while self.asset_queue:
            asset_url = self.asset_queue.popleft()
            resp = self.fetch(asset_url)
            if not resp:
                continue
            content_type = (resp.headers.get("Content-Type") or "").lower()
            is_css = "text/css" in content_type or urlparse(asset_url).path.lower().endswith(".css")
            local_path = url_to_local_path(asset_url, is_page=False)

            if is_css:
                try:
                    css_text = resp.text
                except UnicodeDecodeError:
                    self.write_file(local_path, resp.content)
                    self.downloaded_assets += 1
                    continue
                css_text = rewrite_css_urls(css_text, asset_url, local_path, self.enqueue)
                self.write_file(local_path, css_text.encode("utf-8"))
            else:
                self.write_file(local_path, resp.content)
            self.downloaded_assets += 1

            if self.downloaded_assets % 100 == 0:
                print(f"[assets] {self.downloaded_assets} done")

        print("\nMirror completed.")
        print(f"Pages downloaded: {self.downloaded_pages}")
        print(f"Assets downloaded: {self.downloaded_assets}")
        print(f"Failures: {len(self.failed)}")
        if self.failed:
            with open("mirror_failures.log", "w", encoding="utf-8") as f:
                for url, reason in self.failed:
                    f.write(f"{url}\t{reason}\n")
            print("Failure log: mirror_failures.log")


def main() -> int:
    start = time.time()
    m = Mirror()
    m.crawl()
    elapsed = time.time() - start
    print(f"Elapsed: {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
