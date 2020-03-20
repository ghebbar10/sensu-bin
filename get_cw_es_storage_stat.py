import sys
import getopt
import time
from datetime import datetime, timedelta
import boto3

def get_cw_es_storage_stat(namespace,dimensions_n,dimension_v,dimension_c,metric,stat,crit,warn,region,profile):
    session = boto3.Session(profile_name= profile)
    client = session.client('cloudwatch',region)
    storageval=[]
    response= []
    response=client.get_metric_statistics(
    Namespace=namespace,
        Dimensions=[
            {
                'Name': dimensions_n,
                'Value': dimension_v
            },
            {
                'Name': 'ClientId',
                'Value': dimension_c
            }
        ],
        MetricName=metric,
        StartTime=datetime.now() - timedelta(minutes=10),
        EndTime=datetime.now() - timedelta(minutes=5),
        Period=300,
        Statistics=[
            stat
        ]
    )
    for x in range(len(response['Datapoints'])):
        storageval.append(response['Datapoints'][x][stat])
        
    storageval.sort(reverse=True)
    
    domain = session.client('es',region)
    domain_res = []
    domain_res = domain.describe_elasticsearch_domain(DomainName=dimension_v)
    total_disk = []
    total_disk.append(domain_res['DomainStatus']['EBSOptions']['VolumeSize'])
    rem_per= int(storageval[0]*100 / (total_disk[0] * 1024))
    
    if rem_per >= crit:
        print("FreeStorageSpace for domain " + dimension_v + "is" + str(storageval[0]) + "out of" + str(total_disk[0]) )
        sys.exit(2)
    elif rem_per >= warn:
         print("FreeStorageSpace for domain " + dimension_v + "is" + str(storageval[0]) + "out of" + str(total_disk[0]))
         sys.exit(1)
    else:
        print("FreeStorageSpace for domain " + dimension_v + "is" + str(storageval[0]) + "out of" + str(total_disk[0]))
        sys.exit(0)


def display_help():
    print("\nUsage: python ./get_cw_es_storage_stat [OPTION]...\n")
    print("Checks for the free storage space for mentioned ElasticSearch Domain.")
    print("OPTIONS are: ")
    print("\t python ./get_cw_es_storage_stat -h, --help        \t\t\t Display this help and exit")
    print("\t python ./get_cw_es_storage_stat -r, --region\t\t\t AWS region name. For example 'eu-west-1'. This option is compulsory.")
    print("\t python ./get_cw_es_storage_stat -n, --namespace\t\t\t Cloudwatch Namespace")
    print("\t python ./get_cw_es_storage_stat -d, --dimensions_n\t\t\t Cloudwatch Dimension Name")
    print("\t python ./get_cw_es_storage_stat -i, --clientId\t\t\t Client ID")
    print("\t python ./get_cw_es_storage_stat -v, --dimensions_v\t\t\t Cloudwatch Dimension Value")
    print("\t python ./get_cw_es_storage_stat -m, --metric\t\t\t Cloudwatch Metric")
    print("\t python ./get_cw_es_storage_stat -p, --profile      \t\t\t AWSCLI profile")
    print("\t python ./get_cw_es_storage_stat -s, --stat        \t\t\t Cloudwatch Statistics")
    print("\t python ./get_cw_es_storage_stat -c, --critical    \t\t\t Critical Value")
    print("\t python ./get_cw_es_storage_stat -w, --warning    \t\t\t Warning Value")
    
    print("Examples:\n")
    print("To check free storage space of a domain in a period 5 mins")
    print("\t python ./get_cw_es_storage_stat -r 'eu-west-1' -n 'AWS/ES' -d 'DomainName' -v 'rxit-laas-v5-5' -m 'FreeStorageSpace' -p 'rds' -s 'Minimum' -c 10 -w 20\n")
    print("Git Repository:\n")
    print("\t https://code.edifix.io/OT/probes/\n")

def main():
    try:
        options, args = getopt.getopt(sys.argv[1:],'r:n:d:i:v:m:p:s:c:w:h', ['region=','namespace=','dimensions_n=','clientId=','dimensions_v=','metric=','profile','stat','critical=','warning=','help'])
        for opt, arg in options:
            if opt in ('-h', '--help'):
                display_help()
                sys.exit(2)
            elif opt in ('-r', '--region'):
                region= arg
            elif opt in ('-n', '--namespace'):
                namespace = arg
            elif opt in ('-d', '--dimensions_n'):
                dimensions_n=arg
            elif opt in ('-i', '--clientId'):
                dimensions_n=arg
            elif opt in ('-v', '--dimensions_v'):
                dimensions_v=arg
            elif opt in ('-m', '--metric'):
                metric=arg
            elif opt in ('-p', '--profile'):
                period=arg
            elif opt in ('-s', '--stat'):
                stat=arg
            elif opt in ('-c', '--critical'):
                crit=arg
            elif opt in ('-w', '--warning'):
                warn=arg
            else:
                display_help()
                sys.exit(2)
            
        get_cw_es_storage_stat(namespace,dimensions_n,dimensions_v,dimension_c,metric,stat,int(crit),int(warn),region,profile)
            
    except Exception as e:
        #Print a message or do something useful
        print('Something went wrong! Check Usage with -h switch')
        sys.exit(2)
        
if __name__ == '__main__':
    main()
