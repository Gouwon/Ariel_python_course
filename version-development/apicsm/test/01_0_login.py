#!/usr/bin/python3

import requests, json, ruamel.yaml, sys

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

CONF="cisco_apic_conf.yaml"
with open(CONF, 'r') as cfg_yaml:
    cfg = ruamel.yaml.load(cfg_yaml, Loader=ruamel.yaml.RoundTripLoader)




## variable
CMD_API = 'aaaLogin.json'

METHOD="POST"
req = { 'aaaUser': { 'attributes': { 'name': cfg['cisco_apic_id'], 'pwd': cfg['cisco_apic_pw'] } } }
req_json = json.dumps(req);

# METHOD="GET"
QUERY = ""

COOKIE = {}
if "cisco_apic_key" in cfg:
    COOKIE['APIC-Cookie'] = cfg['cisco_apic_key']

_PORT = cfg['cisco_apic_port']


## API Call
if METHOD == "POST":
    api_url = 'https://' + cfg['cisco_apic_ip'] + ':%s/api/'%str(_PORT) + CMD_API
    resp = requests.post(api_url, verify=False, data=req_json)
else:
    api_url = 'https://' + cfg['cisco_apic_ip'] + ':%s/api/'%str(_PORT) + CMD_API
    if QUERY != None and QUERY != "" :
        api_url = api_url + "?" + QUERY
    resp = requests.get(api_url, verify=False, cookies=COOKIE)
print("[URL] " + api_url)
print("[RES] " + resp.text)
resp_json = json.loads(resp.text)




## Response Handle
_auth_key = resp_json["imdata"][0]["aaaLogin"]["attributes"]["token"]
cfg['cisco_apic_key'] = _auth_key
with open(CONF, 'w') as cfg_yaml:
    ruamel.yaml.dump(cfg, cfg_yaml, Dumper=ruamel.yaml.RoundTripDumper)





