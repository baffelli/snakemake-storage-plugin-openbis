from pybis import Openbis
import requests
def login_anom(ob: Openbis, **kwargs) -> Openbis:
    """
    Trick to allow anonymous login to openBIS
    because pyBIS does not allows this currently
    """
    print(ob.url + ob.as_v3)
    req = requests.post(
        ob.url + ob.as_v3,
        json={
            "method": "loginAsAnonymousUser",
            "jsonrpc": "2.0",
            "id": "1.0",
            "params": [],
        },
    )
    res = req.json()
    if res is not None and "result" in res.keys():
        token = res.get("result")
        ob_new = Openbis(ob.url, **kwargs, token=token)
        return ob_new