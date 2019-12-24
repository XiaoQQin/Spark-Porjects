#coding=UTF-8
import random
import time
import sys

url_paths=[
	"class/112.html",
	"class/128.html",
	"class/145.html",
	"class/146.html",
	"class/131.html",
	"class/130.html",
	"learn/821",
	"course/list"
]

ip_slices=[132,156,124,10,29,167,143,187,30,46,55,63,72,87,98]

http_referers=[
    "https://www.baidu.com/s?wd={query}",
    "https://www.sogou.com/web?query={query}",
    "https://cn.bing.com/search?q={query}",
    "https://search.yahoo.com/search?p={query}"
]
search_keywords=[
	"Spark SQL Action",
	"Hadoop Framework",
	"Storm Action",
	"Spark Streaming Action"
]

status_codes=["200","404","500"]

def sample_url():
	return random.sample(url_paths,1)[0]

def sample_ip():
	slice=random.sample(ip_slices,4)
	return ".".join([str(item) for item in slice])

def sample_referer():
	if random.uniform(0,1)>0.2:
		return "-"
	refer_str=random.sample(http_referers,1)[0]
	query_str=random.sample(search_keywords,1)[0]
	return refer_str.format(query=query_str)

def sample_status_code():
	return random.sample(status_codes,1)[0]

def generate_log(count=10):
	time_str=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
	if len(sys.argv) !=2 :
		print("ERROR! you must input a parameter as log file to output")
		exit(1)
	with open(sys.argv[1],"w") as f:
		while count>=1:
			quary_log="{ip}\t{local_time}\t\"GET /{url} HTTP/1.1\"\t{referer}\t{status_code}".format(local_time=time_str,url=sample_url(),ip=sample_ip(),
				referer=sample_referer(),status_code=sample_status_code())
			count=count-1
			f.write(quary_log+"\n")

if __name__ == '__main__':
	generate_log(100)
