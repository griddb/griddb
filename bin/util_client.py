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
import ssl
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
    # SSL接続有効／無効でデフォルトとして用いるポート番号が異なるため、
    # ここではデフォルトのポート番号は設定しない
    # parser.set_defaults(server="127.0.0.1", port="10040")
    parser.set_defaults(server="127.0.0.1")
    parser.add_option("-u", "--user", metavar="USER/PASS", type="string",
                      action="callback", callback=checkuser,
                      help='Specify the user and password.')
    parser.add_option("--no-proxy", dest="no_proxy",
                      action="store_true", default=False,
                      help='If specified, the proxy will not be used.')
    parser.add_option("--ssl", dest="enable_ssl",
                      action="store_true", default=False,
                      help='If specified, use encrypted connections.')
    parser.add_option("--ssl-verify", dest="enable_ssl_verify",
                      action="store_true", default=False,
                      help='If specified, use encrypted connections and verify certs.')
    parser.set_defaults(username=None, password=None)
            
def decide_use_ssl_and_port(options):
    # REST通信時SSL接続を行うか判断 デフォルトのポート番号を設定
    
    # オプション--ssl --ssl-verify 双方の指定がある場合はエラーとする
    if options.enable_ssl and options.enable_ssl_verify:
        print("A00114: Don't specify both ssl and ssl-verify.") 
        sys.exit(2)
    
    # 環境変数 GS_SYSTEM_SSL 廃止 環境変数 GS_SSL_MODE とする
    if "GS_SSL_MODE" in os.environ:
        # 環境変数 GS_SSL_MODE の値 DISABLED/REQUIRED/VERIFY 以外の場合はエラー
        if os.environ["GS_SSL_MODE"] not in ["DISABLED", "REQUIRED", "VERIFY"]:
            print("A00113: Specify an environment variable GS_SSL_MODE DISABLED or REQUIRED or VERIFY.") 
            sys.exit(2)
#    if "GS_SYSTEM_SSL" in os.environ:
#        # 環境変数 GS_SYSTEM_SSL の値 DISABLED/ENABLED 以外の場合はエラー
#        if os.environ["GS_SYSTEM_SSL"] not in ["DISABLED", "ENABLED"]:
#            print u"A00112: Specify an environment variable GS_SYSTEM_SSL ENABLED or DISABLED." 
#            sys.exit(2)

    # SSL接続無効とするかSSL接続有効とするか決める
    # 環境変数 GS_SYSTEM_SSL 廃止 環境変数 GS_SSL_MODE とする
    if options.enable_ssl:
        # オプション--sslの指定がある場合、SSL接続有効
        options.system_ssl = True
        options.system_ssl_verify = False
    elif options.enable_ssl_verify:
        # オプション--ssl-verifyの指定がある場合、SSL接続有効
        options.system_ssl = True
        options.system_ssl_verify = True
    elif "GS_SSL_MODE" in os.environ:
        # オプション--sslの指定がない場合
        # オプション--ssl-verifyの指定がない場合
        if os.environ["GS_SSL_MODE"] == "REQUIRED":
            options.system_ssl = True
            options.system_ssl_verify = False
        elif os.environ["GS_SSL_MODE"] == "VERIFY":
            options.system_ssl = True
            options.system_ssl_verify = True
        else:
            options.system_ssl = False
            options.system_ssl_verify = False
#    elif "GS_SYSTEM_SSL" in os.environ:
#        # オプション--sslの指定がない場合
#        # 環境変数GS_SYSTEM_SSL：ENABLEDの場合、SSL接続有効
#        # 環境変数GS_SYSTEM_SSL：DISABLEDの場合、SSL接続無効
#        if os.environ["GS_SYSTEM_SSL"] == "ENABLED":
#            options.system_ssl = True
#        else:
#            options.system_ssl = False
    else:
        # 環境変数GS_SYSTEM_SSLの設定値もない場合、SSL接続無効
        options.system_ssl = False
        options.system_ssl_verify = False

    if options.system_ssl and options.system_ssl_verify == False:
        # SSL接続有効 証明書検証無効の場合、HTTPSの証明書の検証をオフにする
        set_not_verify_https()
    if options.system_ssl and options.system_ssl_verify:
        # SSL接続有効 証明書検証有効の場合、HTTPSの証明書の検証はオンとして、ホスト名検証をオフにする
        set_verify_not_check_hostname_https()
    
    # ポート番号の指定がなかった場合、使用するポート番号を決める
    if options.port is None:
        if options.system_ssl:
            # SSL接続する設定がされていた場合はデフォルトのポート番号は10045
            options.port = 10045
        else:
            # SSL接続する設定がない場合はデフォルトのポート番号は10040
            options.port = 10040

def set_not_verify_https():
    # HTTPSの証明書の検証を行わないよう設定
    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        # HTTPS デフォルトで証明書の検証を行わない環境の場合何もしない
        pass
    else:
        # HTTPS 証明書の検証を行わない
        ssl._create_default_https_context = _create_unverified_https_context

def create_verified_not_check_hostname_context():
    # SSL コンテキストを作成
    context = ssl.create_default_context()
    # SSL コンテキストにホスト名検証をオフにする設定を行う
    context.check_hostname = False
    return context

def set_verify_not_check_hostname_https():
    # HTTPSの証明書の検証は行うがホスト名検証は行わないよう設定
    ssl._create_default_https_context = create_verified_not_check_hostname_context


def request_rest(method, path, data, options, log):
    '''
    サーバに対してRESTのリクエストを行う。
    methodに"GET"、"POST"または"POST_JSON"を指定する。
    "POST_JSON"が指定された場合は、メディアタイプをJSONとする。
    戻り値は、レスポンスボディとHTTPステータスコードからなるタプル。
    正常終了であればHTTPステータスコードは200。
    レスポンスボディは正常終了以外ではNone。
    '''
    url = "http://%s:%s%s" % (options.server, options.port, path)
    # SSL接続有効の場合、urlはhttps://…にする
    if options.system_ssl:
        url = "https://%s:%s%s" % (options.server, options.port, path)

    passman = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    passman.add_password(None, url, options.username, options.password)
    authhandler = urllib.request.HTTPBasicAuthHandler(passman)
    if options.no_proxy:
        proxyhandler = urllib.request.ProxyHandler({})
        if options.system_ssl:
            # SSL接続有効時
            opener = urllib.request.build_opener(urllib.request.HTTPSHandler, proxyhandler, authhandler)
        else:
            # SSL接続無効時
            opener = urllib.request.build_opener(proxyhandler, authhandler)
    else:
        if options.system_ssl:
            # SSL接続有効時
            opener = urllib.request.build_opener(urllib.request.HTTPSHandler, authhandler)
        else:
            # SSL接続無効時
            opener = urllib.request.build_opener(authhandler)
    urllib.request.install_opener(opener)
    
    httperr_msg_dict = {
        100: "A01100: The specified cluster name is invalid..",
        101: "A01101: The specified cluster name is invalid.",
        102: "A01101: The specified cluster name is invalid.",
        200: "A30120: Failed to backup.",
        201: "A30121: Specify a backup name.",
        202: "A30122: Failed to create the backup directory.",
        203: "A30123: The specified backup name already exists.",
        204: "A30124: Shutdown process is already running.",
        205: "A30125: The modes cannot be specify at same time.",
        210: "A30130: Specify the latest baseline backup name.",
        211: "A30131: The latest backup data is not backup data of 'since'.",
        212: "A30132: The 'since' backup  cannot execute of because of change of the partition status.",
        213: "A30133: The backup of mode 'since' is running or suspended. Check log file.",
        400: "A01102: The request cannot be executed in recovery processing."
    }

    request_rest_log_data = False
    log_msg = "try to urlopen [no_proxy]:" + str(options.no_proxy) + " [method]:" + method + " [URL]:" + url
    if request_rest_log_data:
        log_msg = log_msg + " [data]:" + str(data)
    log.info(log_msg)
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
        if method == "POST_JSON_NOT_STR":
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
                        master = res_json.get("master")
                        if master != None and master == "undef":
                            err_message = "A00111: The master node is undefined."
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
        #print u"運用コマンドの利用には、環境変数の設定が必要です。"
        return True

    homedir = os.environ["GS_HOME"]
    logdir = os.environ["GS_LOG"]

    if not os.path.exists(homedir):
        err_envs.append("GS_HOME")
    if not os.path.exists(logdir):
        err_envs.append("GS_LOG")
    if err_envs:
        print("A00106: Specify an environment variable for the operating commands. (%s)" % ", ".join(err_envs))
        #print u"運用コマンドの利用には、環境変数の設定が必要です。"
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

def get_backupstat(budir):
    try:
        bustatfile = os.path.join(budir, "gs_backup_info.json")
        if os.path.exists(bustatfile):
            with open(bustatfile) as f:
                l = f.readline()
                bustat = json.loads(l)
        else:
            bustat = None
    except ValueError:
        return 0
    if bustat is None:
        return None
    return bustat.get("backupInfo").get("backupStatus")

def check_backup_stat(budir):
    bustat = get_backupstat(budir)
    if bustat is None:
        return None
    elif bustat == "end":
        return 1
    return 0

#
# 稼働中ノードの現在のバックアップ処理状態を返します。
#  [return]
#   5    : バックアップ　L1C0実行中  incremental
#   4    : バックアップ　L1C1実行中  since
#   3    : バックアップ　L0実行中    Baseline
#   2    : バックアップ処理を実施している
#   1    : バックアップ処理を実施していない
#   0    : サーバが停止している
#   None : 異常終了
#
def check_backup_stat_server(options,log):
    (res, code) = request_rest("GET", "/node/stat", None, options, log)
    if code == 0:
            return 0
    elif res is None:
            return None
    result = json.loads(res)
    mode = result.get("checkpoint").get("mode")
    endTime = result.get("checkpoint").get("endTime")
        
    if ((mode in ["BACKUP","INCREMENTAL_BACKUP_LEVEL_0","INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE","INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL"]) and (endTime != 0))\
    or (mode not in ["BACKUP","INCREMENTAL_BACKUP_LEVEL_0","INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE","INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL"]):
            return 1
    else:
        if (mode == "INCREMENTAL_BACKUP_LEVEL_0"): return 3
        if (mode == "INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE"): return 4
        if (mode == "INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL"): return 5
    return 2
         

def wait_error_case(e,log,msg):
    if e == 0:
        print(msg)
    if e == 1:
        print("\nA00107: Check the status of the node.")
    if e == 2:
        print("\nA00108: Timeout expired. Check the status of the node.")
        log.error("TimeoutError")
        
# addressTypeの種類
addr_type_list = ["system", "cluster", "transaction", "sync"]

# --address-type オプションチェック
def check_addr_type(option, opt_str, value, parser):
    if (value in addr_type_list):
        parser.values.addr_type = value
        return 0
    else:
        print("A00109: The %s can be specified to address type. " % "/".join(map(str, addr_type_list)) +opt_str+"="+value+")") 
        sys.exit(2)

# コマンドプロセスのPIDファイル作成
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

