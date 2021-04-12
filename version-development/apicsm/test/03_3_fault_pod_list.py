#!/usr/bin/python3

import requests, json, ruamel.yaml, sys

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

CONF="cisco_apic_conf.yaml"
with open(CONF, 'r') as cfg_yaml:
    cfg = ruamel.yaml.load(cfg_yaml, Loader=ruamel.yaml.RoundTripLoader)

_PORT = cfg['cisco_apic_port']
_pod = "pod-1"
if len(sys.argv) > 1 :
    _pod = str(sys.argv[1])


##############
## variable ##
##############
DN = "topology/%s"%_pod
# DN = ""
CMD_API = 'class/%s/faultInfo.json'%(DN)
# FCODE = "F0103" # fltCnwPhysIfDown, cnw:PhysIf
# FCODE = "F0546" # fltEthpmIfPortDownNoInfra, ethpm:If
# FCODE = "F1394" # fltEthpmIfPortDownFabric, ethpm:If

# METHOD="POST"

METHOD="GET"
QUERY = 'order-by=faultInfo.severity|desc'
# QUERY = 'query-target-filter=ne(faultInfo.severity,"cleared")&order-by=faultInfo.severity|desc'
# QUERY = 'query-target-filter=eq(faultInfo.lc,"raised-clearing")&order-by=faultInfo.severity|desc'


COOKIE = {}
if "cisco_apic_key" in cfg:
    COOKIE['APIC-Cookie'] = cfg['cisco_apic_key']



##############
## API Call ##
##############
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



#####################
## Response Handle ##
#####################
# print( json.dumps(resp_json, indent=2) )





