#\ coding=UTF-8

import sys
import locale
import os
import json
import subprocess
import time
import signal
#reload(sys)
#sys.setdefaultencoding(locale.getpreferredencoding())

import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import re
from optparse import OptionParser, OptionValueError
from typing import Optional

def checkserver(option, opt_str, value, parser):
    m = re.match(r"^([^:]+?)(?::(\d+))?$", value)
    if m is None:
        raise OptionValueError("A00001: Specify %s option in the format HOST:PORT." % opt_str)
    parser.values.server = m.group(1)
    if m.group(2) is not None:
        parser.values.port = m.group(2)

def checkuser(option, opt_str, value, parser):
    m = re.match(r"(.+?)/(.+)$", value)
    if m is None:
        raise OptionValueError("A00002: Specify %s option in the format USER/PASS." % opt_str)
    parser.values.username = m.group(1)
    parser.values.password = m.group(2)

CLIENT_OPTIONS_USAGE = "[-s HOST[:PORT] | -p PORT] -u USER/PASS"

def add_client_options(parser):
    parser.add_option("-s", "--server", metavar="HOST[:PORT]", type="string",
                      action="callback", callback=checkserver,
                      help='The host name  or the server name (address) and port number.' +
                           'The value "localhost (127.0.0.1):10040" is used by default.')
    parser.add_option("-p", "--port", type="int", dest="port",
                      help='The server port number (The value 10040 is used by default.)')
    parser.set_defaults(server="127.0.0.1", port="10040")
    parser.add_option("-u", "--user", metavar="USER/PASS", type="string",
                      action="callback", callback=checkuser,
                      help='Specify the user and password.')
    parser.set_defaults(username=None, password=None)
            
def request_rest(method, path, data, options, log):
    '''
    Executes REST request to a node.
    Specifies "GET"or "POST" or "POST_JOSN" as method.
    If method is "POST_JSON", set json to Content-type.
    Returns tuple consists of response body and HTTP status code.
    If successful, HTTP status code is 200.
    Response body is None with the exception of the success.
    '''
    url = "http://%s:%s%s" % (options.server, options.port, path)
    passman = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    passman.add_password(None, url, options.username, options.password)
    authhandler = urllib.request.HTTPBasicAuthHandler(passman)
    opener = urllib.request.build_opener(authhandler)
    urllib.request.install_opener(opener)
    
    httperr_msg_dict = {
        100: "A01100: The specified cluster name is invalid..",
        101: "A01101: The specified cluster name is invalid.",
        102: "A01101: The specified cluster name is invalid.",
        204: "A30124: Shutdown process is already running.",
        205: "A30125: The modes cannot be specify at same time.",
        400: "A01102: The request cannot be executed in recovery processing."
    }

    try:
        if data is None:
            data = {}
        if method == "GET":
            data = urllib.parse.urlencode(data)
            f = urllib.request.urlopen(url + "?%s" % data, timeout=30)
        if method == "POST":
            data = urllib.parse.urlencode(data)
            f = urllib.request.urlopen(url, strToBytes(data), timeout=30)
        if method == "POST_JSON":
            data = json.dumps(data)
            req = urllib.request.Request(url)
            req.add_header('Content-type', 'application/json')
            f = urllib.request.urlopen(req, strToBytes(data), timeout=30)
        return (f.read(), 200)

    except urllib.error.HTTPError as e:
        if e.code == 400:
            if hasattr(e, 'read'):
                err_status = 0
                err_reason = ""
                err_message = None
                res: Optional[str] = bytesToStr(e.read())
                if res != None and res != "":
                    res_json = json.loads(res)
                    err_status = res_json.get("errorStatus")
                    err_reason = res_json.get("reason")
                    err_message = httperr_msg_dict.get(err_status)
            if err_message is None:
                err_message = "A00100: Request error occurred."
            print(err_message)
            # log.error(str(e) + " status: " + str(err_status) + " reason: " + err_reason)
            log.error(str(e) + " reason: " + str(err_reason))
        elif e.code == 401:
            if hasattr(options, "silent"):
                print("A00101: Authentication failed, and suspended to wait.")
                return (None, e.code)
            print("A00102: Authentication failed.")
            print("Check the username and password.")
            log.error(str(e))
        elif e.code == 403 or e.code == 503:
            print("A00110: Check the network setting. (" + str(e) + ")")
            log.error(str(e))
        else:
            print("A00103: The error occurred. (" + str(e) + ")")
            log.error(str(e))
        return (None, e.code)

    except urllib.error.URLError as e:
        if hasattr(options, "silent"):
            return (None, 0)
        print("A00104: Failed to connect to the GridDB node.(node="+options.server+":"+str(options.port)+")")
        print("Check the node status.")
        print("URLError", e)
        log.error("URLError " + str(e)+ " (node="+options.server+":"+str(options.port)+")")
        return (None, 0)

def handler(signum, frame):
    print("")
    exit(128+signum)

def install_sigint_handler():
    signal.signal(signal.SIGINT, handler)

def check_variables():

    err_envs = []

    if "GS_HOME" not in os.environ:
        err_envs.append("GS_HOME")
    if "GS_LOG" not in os.environ:
        err_envs.append("GS_LOG")
    if err_envs:
        print("A00105: Specify an environment variable for the operating commands. (%s)" % ", ".join(err_envs))
        return True

    homedir = os.environ["GS_HOME"]
    logdir = os.environ["GS_LOG"]

    if not os.path.exists(homedir):
        err_envs.append("GS_HOME")
    if not os.path.exists(logdir):
        err_envs.append("GS_LOG")
    if err_envs:
        print("A00106: Specify an environment variable for the operating commands. (%s)" % ", ".join(err_envs))
        return True

    return False

def get_required_env():
    if not check_variables():
        homedir = os.environ["GS_HOME"]
        logdir = os.environ["GS_LOG"]
        return (homedir,logdir)
    else:
        sys.exit(1)

def masterexists(options,log):
    (res, code) = request_rest("GET", "/node/stat", None, options, log)
    if res is None:
        return 0
    stat = json.loads(res)
    if "master" in stat.get("cluster"):
        return 1

def optional_arg(arg_default):
    def func(option,opt_str,value,parser):
        if parser.rargs and not parser.rargs[0].startswith('-'):
            val=parser.rargs[0]
            parser.rargs.pop(0)
        else:
            val=arg_default
        setattr(parser.values,option.dest,val)
    return func

def getpid(path):
    if os.path.exists(path):
        fp = open(path, 'rU')
        pid = fp.readline()
        return pid.rstrip()

def psexists(pid, name):
    p1 = subprocess.Popen(['ps', '-p', pid],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            close_fds=True)
    p2 = subprocess.Popen(['grep', name],
                            stdin=p1.stdout,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            close_fds=True)
    line = p2.stdout.readline()
    if line:
        return 1
    return 0

def wait_one_sec():
    time.sleep(1)
    sys.stdout.write(".")
    sys.stdout.flush()

def get_rest_addr_list(options,log):
    addr = []
    port = []
    (a,p) = get_master_addr(options,log)
    addr.append(a)
    port.append(p)
    if addr is None:
        return (None,None)
    options.server = addr[0]
    options.port = port[0]
    (addr,port) = get_follower_addr(options,log,addr,port)
    return (addr,port)

def get_master_addr(options,log):
    data = { "addressType" :"system" }
    (res, code) = request_rest("GET", "/node/stat", data, options, log)
    if res is None:
        return (None,None)
    nodestat = json.loads(res)
    addr = nodestat.get("cluster").get("master").get("address")
    port = int(nodestat.get("cluster").get("master").get("port"))
    return (addr,port)

def get_follower_addr(options,log,addr,port):
    (res, code) = request_rest("GET", "/node/host", None, options, log)
    if res is None:
        return (None,None)
    nodestat = json.loads(res)
    i = 0
    while i < len(nodestat.get("follower","")):
        addr.append(nodestat.get("follower")[i].get("address"))
        port.append(int(nodestat.get("follower")[i].get("port")))
        i += 1
    return (addr,port)
    
def get_nodestat(options,log) -> Optional[str]:
    (res, code) = request_rest("GET", "/node/stat", None, options, log)
    if res is None:
        return None
    nodestat = json.loads(res)
    return nodestat.get("cluster").get("nodeStatus")
    
def get_clusterstat(options,log) -> Optional[str]:
    (res, code) = request_rest("GET", "/node/stat", None, options, log)
    if res is None:
        return None
    nodestat = json.loads(res)
    return nodestat.get("cluster").get("clusterStatus")

def get_cluster_name(options,log) -> Optional[str]:
    (res, code) = request_rest("GET", "/node/stat", None, options, log)
    if res is None:
        return None
    nodestat = json.loads(res)
    return nodestat.get("cluster").get("clusterName")
    
def get_active_num(options,log):
    (res, code) = request_rest("GET", "/node/stat", None, options, log)
    if res is None:
        return None
    nodestat = json.loads(res)
    return nodestat.get("cluster").get("activeCount")
    
def get_designated_num(options,log):
    (res, code) = request_rest("GET", "/node/stat", None, options, log)
    if res is None:
        return None
    nodestat = json.loads(res)
    return nodestat.get("cluster").get("designatedCount")
    
def get_critical_node_addr(options,log):
    # To get list of the cluster address of the critical node
    addr = []
    (res, code) = request_rest("GET", "/node/partition", None, options, log)
    if res is None:
        return None
    pstat = json.loads(res)
    i = 0
    while i < len(pstat):
        ownerExists = True
        backupExists = True
        
        owner = pstat[i].get("owner")
        if owner is None :
            ownerExists = False
        
        backup = pstat[i].get("backup")
        if len(backup) == 0:
            backupExists = False
            
        if ownerExists and (not backupExists):
            addr.append([owner.get("address"), owner.get("port")])
            
        if (not ownerExists) and backupExists:
            for b in backup:
                addr.append([b.get("address"), b.get("port")])

        i += 1
    return addr
                
def is_critical_node(options,log):
    (res, code) = request_rest("GET", "/node/stat", None, options, log)
    if res is None:
        return None
    nodestat = json.loads(res)
    # nodeList[0] is always own cluster address
    caddr = nodestat.get("cluster").get("nodeList")[0]
    addr = get_critical_node_addr(options, log)
    i = 0
    
    myselfAddress = [caddr.get("address"), caddr.get("port")]
    if myselfAddress in addr:
        return 1
    return 0
    
def is_active_cluster(options,log):
    clusterstat: Optional[str] = get_clusterstat(options,log)
    if clusterstat is None:
        return None
    if clusterstat == "MASTER" or clusterstat == "FOLLOWER":
        return 1
    return 0

def wait_error_case(e,log,msg):
    if e == 0:
        print(msg)
    if e == 1:
        print("\nA00107: Check the status of the node.")
    if e == 2:
        print("\nA00108: Timeout expired. Check the status of the node.")
        log.error("TimeoutError")
        
addr_type_list = ["system", "cluster", "transaction", "sync"]

# checks --address-type option
def check_addr_type(option, opt_str, value, parser):
    if (value in addr_type_list):
        parser.values.addr_type = value
        return 0
    else:
        print("A00109: The %s can be specified to address type. " % "/".join(map(str, addr_type_list)) +opt_str+"="+value+")") 
        sys.exit(2)

def write_pidfile(pidfile):
    pid = os.getpid()
    fp = open(pidfile, "w")
    try:
        fp.write(str(pid))
        return True
    finally:
        fp.close()


# Get /node/stat/checkpoint
def get_cp_stats(options,log):
    (res, code) = request_rest("GET", "/node/stat", None, options, log)
    if res is None:
        return None
    nodestat = json.loads(res)
    return nodestat.get("checkpoint")

# Wait for some cp operation to complete
def is_cp_completed(options, log, cp_type, cp_count):
    cp_stats = get_cp_stats(options, log)
    latest_cp_count = cp_stats.get(cp_type)
    return latest_cp_count > cp_count



def bytesToStr(value) -> Optional[str]:
    rval = value
    vtype = type(value)
    if vtype is bytes:
        rval = value.decode()
    return rval

def strToBytes(value):
    rval = value
    vtype = type(value)
    if vtype is str:
        rval = value.encode()
    return rval

