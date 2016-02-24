#\ coding=UTF-8

import sys
import locale
import os
import json
import subprocess
import time
import signal
reload(sys)
sys.setdefaultencoding(locale.getpreferredencoding())

import urllib
import urllib2
import re
from optparse import OptionParser, OptionValueError

def checkserver(option, opt_str, value, parser):
	m = re.match(r"^([^:]+?)(?::(\d+))?$", value)
	if m is None:
		raise OptionValueError(u"A00001: If you use %s option, specify HOST or HOST:PORT style." % opt_str)
	parser.values.server = m.group(1)
	if m.group(2) is not None:
		parser.values.port = m.group(2)

def checkuser(option, opt_str, value, parser):
	m = re.match(r"(.+?)/(.+)$", value)
	if m is None:
		raise OptionValueError(u"A00002: If you use %s option, specify USER/PASS style." % opt_str)
	parser.values.username = m.group(1)
	parser.values.password = m.group(2)

CLIENT_OPTIONS_USAGE = "[-s HOST[:PORT] | -p PORT] -u USER/PASS"

def add_client_options(parser):
	parser.add_option("-s", "--server", metavar="HOST[:PORT]", type="string",
	                  action="callback", callback=checkserver,
	                  help=u'host name or IP address with port number ' +
	                       u'(default value is 127.0.0.1:10040)')
	parser.add_option("-p", "--port", type="int", dest="port",
	                  help=u'port number (default value is 10040)')
	parser.set_defaults(server="127.0.0.1", port="10040")
	parser.add_option("-u", "--user", metavar="USER/PASS", type="string",
	                  action="callback", callback=checkuser,
	                  help=u'user name and password')
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
	passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
	passman.add_password(None, url, options.username, options.password)
	authhandler = urllib2.HTTPBasicAuthHandler(passman)
	opener = urllib2.build_opener(authhandler)
	urllib2.install_opener(opener)
	
	httperr_msg_dict = {
		100: u"A01100: Specified cluster name is wrong.",
		101: u"A01101: Specified cluster name is wrong.",
		102: u"A01101: Specified cluster name is wrong.",
		204: u"A30124: Shutdown is already in progress.",
		205: u"A30125: You can't use both modes.",
		400: u"A01102: You can't execute the command because during recovery."
	}

	try:
		if data is None:
			data = {}
		if method is "GET":
			data = urllib.urlencode(data)
			f = urllib2.urlopen(url + "?%s" % data, timeout=30)
		if method is "POST":
			data = urllib.urlencode(data)
			f = urllib2.urlopen(url, data, timeout=30)
		if method is "POST_JSON":
			data = json.dumps(data)
			req = urllib2.Request(url)
			req.add_header('Content-type', 'application/json')
			f = urllib2.urlopen(req, data, timeout=30)
		return (f.read(), 200)

	except urllib2.HTTPError, e:
		if e.code == 400:
			if hasattr(e, 'read'):
				err_status = 0
				err_reason = ""
				err_message = None
				res = e.read()
				if res != None and res != "":
					res_json = json.loads(res)
					err_status = res_json.get("errorStatus")
					err_reason = res_json.get("reason")
					err_message = httperr_msg_dict.get(err_status)
			if err_message is None:
				err_message = u"A00100: Failed request."
			print err_message
			# log.error(str(e) + " status: " + str(err_status) + " reason: " + err_reason)
			log.error(str(e) + " reason: " + err_reason)
		elif e.code == 401:
			if hasattr(options, "silent"):
				print u"A00101: Stops without wait, because authentication error occurred."
				return (None, e.code)
			print u"A00102: Authentication error occurred."
			print u"Confirm user name and password."
			log.error(str(e))
		elif e.code == 403 or e.code == 503:
			print u"A00110: Confirm network setting. (" + str(e) + ")"
			log.error(str(e))
		else:
			print u"A00103: Error occurred (" + str(e) + ")."
			log.error(str(e))
		return (None, e.code)

	except urllib2.URLError, e:
		if hasattr(options, "silent"):
			return (None, 0)
		print u"A00104: Failed to connect node. (node="+options.server+":"+str(options.port)+")"
		print u"Confirm node is started."
		print u"URLError", e
		log.error("URLError " + str(e)+ " (node="+options.server+":"+str(options.port)+")")
		return (None, 0)

def handler(signum, frame):
	print ""
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
		print u"A00105: %s environment variable not set." % ", ".join(err_envs)
		return True

	homedir = os.environ["GS_HOME"]
	logdir = os.environ["GS_LOG"]

	if not os.path.exists(homedir):
		err_envs.append("GS_HOME")
	if not os.path.exists(logdir):
		err_envs.append("GS_LOG")
	if err_envs:
		print u"A00106: Directory of %s environment variable does not exist." % ", ".join(err_envs)
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
	if stat.get("cluster").has_key("master"):
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
	
def get_nodestat(options,log):
	(res, code) = request_rest("GET", "/node/stat", None, options, log)
	if res is None:
		return None
	nodestat = json.loads(res)
	return nodestat.get("cluster").get("nodeStatus")
	
def get_clusterstat(options,log):
	(res, code) = request_rest("GET", "/node/stat", None, options, log)
	if res is None:
		return None
	nodestat = json.loads(res)
	return nodestat.get("cluster").get("clusterStatus")

def get_cluster_name(options,log):
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
	clusterstat = get_clusterstat(options,log)
	if clusterstat is None:
		return None
	if clusterstat == "MASTER" or clusterstat == "FOLLOWER":
		return 1
	return 0

def wait_error_case(e,log,msg):
	if e == 0:
		print msg
	if e == 1:
		print u"\nA00107: Confirm node status."
	if e == 2:
		print u"\nA00108: Timeout occurred. Confirm node status."
		log.error("TimeoutError")
		
addr_type_list = ["system", "cluster", "transaction", "sync"]

# checks --address-type option
def check_addr_type(option, opt_str, value, parser):
	if (value in addr_type_list):
		parser.values.addr_type = value
		return 0
	else:
		print u"A00109: The value of address-type option must be %s. (" % "/".join(map(str, addr_type_list)) +opt_str+"="+value+")" 
		sys.exit(2)

def write_pidfile(pidfile):
	pid = os.getpid()
	fp = open(pidfile, "w")
	try:
		fp.write(str(pid))
		return True
	finally:
		fp.close()
