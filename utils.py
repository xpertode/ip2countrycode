import socket
from geoip import geolite2
from urlparse import urlparse
import logging
import requests,json

logger = logging.getLogger(__name__)

#Proxy Conf
http_proxy = "http://127.0.0.1:8118"
proxyDict = { "http"  : http_proxy }

s = requests.session()
s.proxies = proxyDict

def getIP(domain):
	# api_request  = "http://dig.jsondns.org/IN/%sA"%(domain)
	api_request = "http://api.statdns.com/%sa"%(domain)
	try:
		logger.info("performing DNS lookup of %s",domain)
		# ip = socket.gethostbyname(domain)
		r = s.get(api_request)
		j = json.loads(r.text)
		# logger.info(j)
		ip = j["answer"][-1]["rdata"]
		return ip
	except:
		logger.info("Url not reachable: %s",domain)
		return None

def ip_to_country_code(ip):
	match = geolite2.lookup(ip)
	if match is not None:
		if match.country is not None:
			logger.info("%s maps to %s",ip,match.country)
			return match.country
		elif match.continent is not None:
			logger.info("%s maps to %s",ip,match.continent)
			return match.continent
	logger.info("IP lookup failed in geolite2 for  %s",ip)
	return None

def get_country_code(url):
	parsed_uri = urlparse(url)
	domain = '{uri.netloc}/'.format(uri=parsed_uri)
	ip = getIP(domain)
	if ip is not None:
		logger.info("DNS Lookup successful for %s,IP is %s",domain,ip)
		code = ip_to_country_code(ip)
		return code
	else:
		logger.info("DNS Lookup failed for %s",domain)
		return None

def update_table(ip,sql_id,table="googleimagefound",column="ip"):
	sql = 'update %s set %s = \"%s\" where id = %s;'%(table,column,ip,sql_id)
	return sql

