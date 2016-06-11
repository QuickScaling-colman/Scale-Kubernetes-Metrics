import json
import urllib2
import datetime
from pymongo import MongoClient
import sched
import time
import re

def GetPodsInNameSpace(kubeURI, namespace):
	pods_request = json.load(urllib2.urlopen(kubeURI + "/api/v1/proxy/namespaces/kube-system/services/heapster/api/v1/model/namespaces/" + namespace + "/pods/"))

	return pods_request

def GetPodMetrics(kubeURI, namespace, pod_name, retention):
	
	current_date = datetime.datetime.utcnow()
	current_date_minus_minute = current_date - datetime.timedelta(seconds=retention)
	start_date = current_date_minus_minute.isoformat('T') + "Z"

	cpu_usage_rate_request = json.load(urllib2.urlopen(kubeURI + "/api/v1/proxy/namespaces/kube-system/services/heapster/api/v1/model/namespaces/" + namespace + "/pods/" + pod_name + "/metrics/cpu/usage_rate?start=" + start_date))
	cpu_usage_rate_array = cpu_usage_rate_request['metrics']

	cpu_limit_request = json.load(urllib2.urlopen(kubeURI + "/api/v1/proxy/namespaces/kube-system/services/heapster/api/v1/model/namespaces/" + namespace + "/pods/" + pod_name + "/metrics/cpu/limit?start=" + start_date))
	cpu_limit_array = cpu_limit_request['metrics']

	memory_usage_request = json.load(urllib2.urlopen(kubeURI + "/api/v1/proxy/namespaces/kube-system/services/heapster/api/v1/model/namespaces/" + namespace + "/pods/" + pod_name + "/metrics/memory/usage?start=" + start_date))
	memory_usage_array = memory_usage_request['metrics']

	memory_limit_request = json.load(urllib2.urlopen(kubeURI + "/api/v1/proxy/namespaces/kube-system/services/heapster/api/v1/model/namespaces/" + namespace + "/pods/" + pod_name + "/metrics/memory/limit?start=" + start_date))
	memory_limit_array = memory_limit_request['metrics']

	output = []
	for (current_json_cpu, current_json_cpu_limit, current_json_memory, current_json_memory_limit) \
		in zip(cpu_usage_rate_array, cpu_limit_array, memory_usage_array, memory_limit_array):
		data = {}
		data[u'timestamp'] = current_json_cpu[u'timestamp']
		data[u'cpu_rate'] = current_json_cpu[u'value']
		data[u'cpu_limit'] = current_json_cpu_limit[u'value']
		data[u'memory'] = current_json_memory[u'value']
		data[u'memory_limit'] = current_json_memory_limit[u'value']
		output.append(data)

	return output

def AggregateMetrics(db, kubeURI, namespace, pod_regex, retention):

	pods_names_array = GetPodsInNameSpace(kubeURI, namespace)

	regex = re.compile('.*(%s).*'%pod_regex)

	pods_metrics_array = []
	
	while True:

		for pod in pods_names_array:
			if regex.match(pod) is not None:
				print "Pod selected: %s" % pod
				pods_metrics_array.append(GetPodMetrics(kubeURI, namespace, pod, retention))

		arr_sizes = []
		for pod in pods_metrics_array:
			arr_sizes.append(len(pod))
		
		if arr_sizes:
			break

	min_array_size = min(arr_sizes)

	for i in range(min_array_size):
		cpu_rate_sum = 0
		cpu_limit_sum = 0
		memory_sum = 0
		memory_limit_sum = 0

		for j in range(len(pods_metrics_array)):
			cpu_rate_sum += pods_metrics_array[j][i][u'cpu_rate']
			cpu_limit_sum += pods_metrics_array[j][i][u'cpu_limit']
			memory_sum += pods_metrics_array[j][i][u'memory']
			memory_limit_sum += pods_metrics_array[j][i][u'memory_limit']
			print pods_metrics_array[j][i][u'cpu_rate']
		print "--------------"

		timestamp = datetime.datetime.strptime(pods_metrics_array[j][i][u'timestamp'],'%Y-%m-%dT%H:%M:%SZ')
		record = {'timestamp': timestamp, 'cpu': cpu_rate_sum, 'cpu_limit': cpu_limit_sum, 'memory': memory_sum, 'memory_limit': memory_limit_sum }
		print record
		db.websitesCpuRam.insert_one(record)

if __name__ == "__main__":
        from argparse import ArgumentParser

        parser = ArgumentParser()
        parser.add_argument("mongodb-uri", metavar="MONGODB_URL", help="Mongodb uri example: mongodb://localhost:27017/quickscaling")
        parser.add_argument("kubernetes-master-url", metavar="KUBE_API_URL", help="Kubernetes url example: https://<host>:8080/")
        parser.add_argument("namespace", metavar="NAMESPACE", help="Kubernetes namespace")
        parser.add_argument("pod-regex", metavar="REGEX", help="Pod regex")
        parser.add_argument("write-interval", metavar="WRITE_INTERVAL", type=int, 
			    help="Every WRITE_INTERVAL seoncds metrics record will be writen to database")
        parser.add_argument("retention", metavar="RETENTION", type=int, help="Number of second that will be aggregated")

        args = parser.parse_args()
        argsdict = vars(args)
	
	print "-----------------------------"
        print "mongodb-uri: %s" % argsdict["mongodb-uri"]
        print "kubernetes-master-url: %s" % argsdict["kubernetes-master-url"]
        print "namespace: %s" % argsdict["namespace"]
        print "pod-regex: %s" % argsdict["pod-regex"]
        print "write-interval: %d" % argsdict["write-interval"]
        print "retention: %d" % argsdict["retention"]
	print "-----------------------------"

	connection = MongoClient(argsdict["mongodb-uri"])
	db = connection.quickscaling
	
	def SchedTask(db, kubeURI, namespace, pod_regex, writeInterval, retention):
		AggregateMetrics(db, kubeURI, namespace, pod_regex, retention)
		time.sleep(writeInterval)

	while True:
		SchedTask(db,
			  argsdict["kubernetes-master-url"],
			  argsdict["namespace"],
			  argsdict["pod-regex"],
			  argsdict["write-interval"], 
			  argsdict["retention"])
