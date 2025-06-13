import requests
import json
from typing import Dict, Any, Optional, Union
import pandas as pd

class HTTPClient:
    def __init__(self, base_url: str = "", default_headers: Optional[Dict[str, str]] = None):
        """
        初始化HTTP客户端
        """
        self.base_url = base_url.rstrip('/')
        self.default_headers = default_headers or {}
        self.session = requests.Session()
        
    def _build_url(self, endpoint: str) -> str:
        """构建完整URL"""
        if endpoint.startswith('http'):
            return endpoint
        return f"{self.base_url}/{endpoint.lstrip('/')}" if self.base_url else endpoint
    
    def _merge_headers(self, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
        """合并默认头部和自定义头部"""
        merged = self.default_headers.copy()
        if headers:
            merged.update(headers)
        return merged
    
    def get(self, url: str, headers: Optional[Dict[str, str]] = None, 
            params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> requests.Response:
        """
        发送GET请求
        """
        full_url = self._build_url(url)
        merged_headers = self._merge_headers(headers)
        
        try:
            response = self.session.get(
                full_url,
                headers=merged_headers,
                params=params,
                timeout=timeout
            )
            return response
        except requests.exceptions.RequestException as e:
            print(f"GET请求失败: {e}")
            raise
    
    def post(self, url: str, data: Optional[Union[Dict, str, bytes]] = None,
             json_data: Optional[Dict[str, Any]] = None,
             headers: Optional[Dict[str, str]] = None,
             timeout: int = 30) -> requests.Response:
        """
        发送POST请求
        """
        full_url = self._build_url(url)
        merged_headers = self._merge_headers(headers)
        
        try:
            response = self.session.post(
                full_url,
                data=data,
                json=json_data,
                headers=merged_headers,
                timeout=timeout
            )
            return response
        except requests.exceptions.RequestException as e:
            print(f"POST请求失败: {e}")
            raise
    
    def close(self):
        """关闭session"""
        self.session.close()

    


if __name__ == "__main__":
    # 创建客户端实例
    client = HTTPClient(
        base_url="https://ehall.xjtu.edu.cn/",
        default_headers={"User-Agent": "SimpleHTTPClient/1.0"}
    )
    
    try:
        df = pd.DataFrame()
        for i in range(1, 41):
            form_data = {
                "querySetting": '''[{"name":"SFXGXK","caption":"是否校公选课","linkOpt":"AND","builderList":"cbl_String","builder":"equal","value":"0","value_display":"否"},[{"name":"XNXQDM","value":"2024-2025-2","linkOpt":"and","builder":"equal"},[{"name":"RWZTDM","value":"1","linkOpt":"and","builder":"equal"},{"name":"RWZTDM","linkOpt":"or","builder":"isNull"}]],{"name":"*order","value":"+KKDWDM,+KCH,+KXH","linkOpt":"AND","builder":"m_value_equal"}]''',
                "pageSize": 100,
                "pageNumber": i,
            }
            
            response = client.post("/jwapp/sys/kcbcx/modules/qxkcb/qxfbkccx.do",
                                data=form_data,
                                headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                                        "Cookie":"EMAP_LANG=zh; THEME=cherry; _WEU=uaCHa3hOQY4QqkS2zk5ixqRQvm*pyrWxFe_wYXTdYN252pfywnnIOYoDi94xANRJ8_8D5tzBFkxmO4KgtfgIbXRabwCapbIPUDPP1uxFI_erLryiEOc4aqMHrvS4EGs5MnZu48*U6FXleFB7c6FQor0vhuOGCwX3pIsyxv6HT5EvWNNFR7Uy2*4bTSiAB9Rv_TxX5qDme3ELH69tGJGMQo..; CASTGC=7Ga7V2xykI0mlZ1zo83CIXblD3sKwfUfyIaIObOqXYDuYAnuyeJjhw==; MOD_AMP_AUTH=MOD_AMP_d6f7dcae-78bf-4303-a4bf-7ebdefb82abf; asessionid=9c9d49fe-a78b-4ace-bb9e-d64fa79c81c1; amp.locale=undefined; route=ab22dc972e174017d573ee90262bcc96; JSESSIONID=Ak4z54d1XVMiEf5IIF1kvyb1McM57hIq_vunad3bghYWlKaczQbv!-708124622"})
            
            print(f"状态码: {response.status_code}")
            df2 = pd.DataFrame(response.json()['datas']['qxfbkccx']['rows']).dropna(axis=1, how='all')
            df = pd.concat([df, df2[['KCH', 'KCM', 'SKJS', 'XF', 'XS']]], ignore_index=True)
        df.to_csv("xjtu_courses.csv")
        
    except Exception as e:
        print(f"POST请求出错: {e}\n")
    
    # 关闭客户端
    client.close()
    print("客户端已关闭")