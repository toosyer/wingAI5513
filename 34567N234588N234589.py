"""
多端口輪換搜尋伺服器 - 無結果自動切換端口
端口: 34567 (主) / 23588 / 23589
"""

import json
import re
import time
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import quote

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests
from bs4 import BeautifulSoup

# ========== 配置 ==========
HOST = '127.0.0.1'
PORTS = [34567, 23588, 23589]  # 主端口 + 備用端口
FETCH_TIMEOUT = 4
MAX_PAGES = 8
TARGET_RESULTS = 20
MAX_RETRY_PORTS = 3  # 最多嘗試幾個端口

USER_AGENTS = [
    f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{i}.0.0.0 Safari/537.36"
    for i in range(135, 145)
]

# 全局變量：記錄每個端口的結果
port_results = {port: None for port in PORTS}
port_locks = {port: threading.Lock() for port in PORTS}


class StreamingHandler(BaseHTTPRequestHandler):
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_POST(self):
        if self.path == '/search':
            try:
                length = int(self.headers.get('Content-Length', 0))
                data = json.loads(self.rfile.read(length).decode('utf-8'))
                keyword = data.get('keyword', '').strip()
                
                if not keyword:
                    self._send_json(400, {"success": False, "error": "請輸入關鍵詞"})
                    return
                
                print(f"📨 [{self.server.server_port}] 搜尋: {keyword}")
                
                # 設置流式響應頭
                self.send_response(200)
                self.send_header('Content-Type', 'application/x-ndjson')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Cache-Control', 'no-cache')
                self.end_headers()
                self.wfile.flush()
                
                # 開始搜尋（支援端口輪換）
                self._stream_search_with_fallback(keyword)
                
            except Exception as e:
                print(f"錯誤: {e}")
                self._send_json(500, {"success": False, "error": str(e)})
        else:
            self._send_json(404, {"success": False, "error": "Not Found"})
    
    def _send_event(self, data):
        try:
            self.wfile.write((json.dumps(data, ensure_ascii=False) + "\n").encode('utf-8'))
            self.wfile.flush()
        except:
            pass
    
    def _stream_search_with_fallback(self, keyword):
        """支援端口輪換的搜尋 - 沒結果就切換到下一個端口"""
        start_time = time.time()
        
        # 嘗試的端口列表（從當前端口開始，然後輪換）
        current_port = self.server.server_port
        port_attempts = []
        
        # 找出當前端口在列表中的位置，然後輪換
        try:
            idx = PORTS.index(current_port)
            port_attempts = PORTS[idx:] + PORTS[:idx]
        except ValueError:
            port_attempts = PORTS
        
        print(f"🔄 端口嘗試順序: {port_attempts}")
        
        all_results = []
        used_ports = []
        
        for attempt_port in port_attempts[:MAX_RETRY_PORTS]:
            if attempt_port != current_port:
                print(f"🔄 切換到端口 {attempt_port} 嘗試...")
                self._send_event({
                    "type": "status", 
                    "message": f"切換端口 {attempt_port} 搜尋中..."
                })
            
            # 搜索該端口對應的結果
            results = self._search_with_driver(keyword, attempt_port)
            
            if results:
                print(f"✅ 端口 {attempt_port} 找到 {len(results)} 個結果")
                all_results = results
                used_ports.append(attempt_port)
                break
            else:
                print(f"❌ 端口 {attempt_port} 無結果")
                used_ports.append(attempt_port)
                self._send_event({
                    "type": "status",
                    "message": f"端口 {attempt_port} 無結果，切換下一個..."
                })
                time.sleep(1)  # 切換前等待
        
        if not all_results:
            print(f"⚠️ 所有端口都無結果！")
            self._send_event({"type": "error", "message": "所有端口都未找到結果"})
            self._send_event({"type": "done"})
            return
        
        # 發送開始事件
        self._send_event({"type": "start", "total": len(all_results), "usedPorts": used_ports})
        
        # 逐個抓取詳細內容
        for idx, result in enumerate(all_results):
            fetch_result = self._fetch_one(result)
            if fetch_result:
                self._send_event({
                    "type": "result",
                    "data": fetch_result,
                    "completed": idx + 1,
                    "total": len(all_results)
                })
        
        elapsed = time.time() - start_time
        self._send_event({
            "type": "done",
            "stats": {"totalTime": round(elapsed, 2), "count": len(all_results), "portsUsed": used_ports}
        })
        print(f"✅ 完成 {len(all_results)} 個，耗時 {elapsed:.1f}秒，使用端口: {used_ports}")
    
    def _search_with_driver(self, keyword, port_hint=None):
        """使用 Selenium 搜索多頁，返回結果列表"""
        driver = None
        all_urls = []
        page_num = 1
        
        print(f"📄 端口 {port_hint} 開始搜索，目標 {TARGET_RESULTS} 個...")
        
        while len(all_urls) < TARGET_RESULTS and page_num <= MAX_PAGES:
            print(f"   [端口 {port_hint}] 第 {page_num} 頁...")
            page_results = self._search_page(keyword, page_num, port_hint)
            
            if not page_results:
                print(f"   [端口 {port_hint}] 第 {page_num} 頁無結果，停止")
                break
            
            # 去重添加
            for r in page_results:
                if r['url'] not in [x['url'] for x in all_urls]:
                    all_urls.append(r)
            
            print(f"   [端口 {port_hint}] 累計: {len(all_urls)} 個")
            
            if len(all_urls) >= TARGET_RESULTS:
                break
            
            page_num += 1
            time.sleep(0.5)  # 頁面間隔
        
        return all_urls[:TARGET_RESULTS]
    
    def _search_page(self, keyword, page_num, port_hint=None):
        """搜索單頁"""
        driver = None
        try:
            start = (page_num - 1) * 10
            url = f"https://www.google.com/search?q={quote(keyword)}&hl=zh-TW&start={start}&num=10"
            
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--lang=zh-TW")
            options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            options.add_argument(f"user-agent={USER_AGENTS[page_num % len(USER_AGENTS)]}")
            
            # 添加端口特定的隨機延遲，避免被 ban
            import random
            time.sleep(random.uniform(0.5, 1.5))
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(10)
            driver.get(url)
            time.sleep(2)  # 完整等待
            
            results = []
            elements = driver.find_elements(By.CSS_SELECTOR, "div.g")
            if not elements:
                elements = driver.find_elements(By.CSS_SELECTOR, "div.yuRUbf")
            
            for pos, elem in enumerate(elements[:10], start=1):
                try:
                    title_elem = elem.find_element(By.CSS_SELECTOR, "h3")
                    title = title_elem.text.strip()
                    if not title or len(title) < 2:
                        continue
                    
                    link_elem = elem.find_element(By.CSS_SELECTOR, "a")
                    url = link_elem.get_attribute("href")
                    if not url or "google" in url or "youtube" in url:
                        continue
                    
                    snippet = ""
                    try:
                        snippet_elem = elem.find_element(By.CSS_SELECTOR, ".VwiC3b, .aCOpRe")
                        snippet = snippet_elem.text.strip()[:200]
                    except:
                        pass
                    
                    results.append({
                        "title": title,
                        "url": url,
                        "snippet": snippet,
                        "page": page_num,
                        "position": pos
                    })
                except:
                    continue
            
            return results
        except Exception as e:
            print(f"搜索錯誤: {e}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def _fetch_one(self, result):
        """抓取單個頁面"""
        url = result['url']
        title = result['title']
        snippet = result.get('snippet', '')
        
        headers = {
            'User-Agent': USER_AGENTS[0],
            'Accept-Language': 'zh-TW,zh;q=0.9',
        }
        
        try:
            resp = requests.get(url, timeout=FETCH_TIMEOUT, headers=headers, verify=False, allow_redirects=True)
            
            content = ""
            if resp.status_code == 200:
                resp.encoding = resp.apparent_encoding or 'utf-8'
                soup = BeautifulSoup(resp.text, 'html.parser')
                for tag in soup(['script', 'style', 'nav', 'footer']):
                    tag.decompose()
                
                for selector in ['article', 'main', '.content', '#content', '.post-content']:
                    elem = soup.select_one(selector)
                    if elem:
                        content = elem.get_text(separator=' ', strip=True)
                        break
                
                if not content:
                    p_tags = soup.find_all('p')
                    if p_tags:
                        content = ' '.join([p.get_text(strip=True) for p in p_tags[:5]])
                
                if not content:
                    body = soup.find('body')
                    if body:
                        content = body.get_text(separator=' ', strip=True)
                
                content = re.sub(r'\s+', ' ', content).strip()
                content = content[:200] + "..." if len(content) > 200 else content
                if not content:
                    content = "(無文字內容)"
            else:
                content = f"(HTTP {resp.status_code})"
            
            return {
                "title": title[:80],
                "url": url,
                "snippet": snippet[:100] if snippet else "",
                "content": content[:150],
                "page": result['page'],
                "position": result['position']
            }
        except Exception as e:
            return {
                "title": title[:80],
                "url": url,
                "snippet": snippet[:100] if snippet else "",
                "content": "(抓取失敗)",
                "page": result['page'],
                "position": result['position']
            }
    
    def _send_json(self, code, data):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def log_message(self, format, *args):
        pass


def run_all_servers():
    """啟動多個端口的伺服器"""
    servers = []
    threads = []
    
    for port in PORTS:
        server = HTTPServer((HOST, port), StreamingHandler)
        servers.append(server)
        thread = threading.Thread(target=server.serve_forever, name=f"Server-{port}")
        thread.daemon = True
        threads.append(thread)
        thread.start()
        print(f"✅ 伺服器已啟動: http://{HOST}:{port}")
    
    print("=" * 40)
    print("🚀 多端口流式搜尋伺服器")
    print(f"📡 端口: {PORTS}")
    print(f"🎯 目標: {TARGET_RESULTS} 個/頁")
    print(f"🔄 無結果自動切換下一個端口")
    print("=" * 40)
    
    # 保持主線程運行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 關閉所有伺服器...")
        for server in servers:
            server.shutdown()


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings()
    run_all_servers()