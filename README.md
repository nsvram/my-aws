# my-aws


region_name = "ap-southeast-2"
emr_client = boto3.client('emr', region_name=region_name)


def create_cluster():
    client = boto3.client('emr', region_name=region_name)
    response = client.run_job_flow(
            Name="Mycluster",
            LogUri="s3://1.venkat/emr/logs/",
            ReleaseLabel="emr-5.13.0",
            JobFlowRole='EMR_EC2_DefaultRole',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'emr-master',
                        'InstanceRole': 'MASTER',
                        'InstanceType': "m3.xlarge",
                        'InstanceCount': 1,
                            
                    },
                    {
                        'Name': 'emr-core',
                        'InstanceRole': 'CORE',
                        'InstanceType': "m3.xlarge",
                        'InstanceCount': 2,
                    },
                ],
                'Ec2KeyName': "nsvram-emr",
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': "subnet-3afa4973",
                #'ServiceAccessSecurityGroup': service_access_security_group,
                'EmrManagedMasterSecurityGroup': "sg-568dd72f",
                #'AdditionalMasterSecurityGroups': master_additional_security_group,
                'EmrManagedSlaveSecurityGroup': "sg-0480da7d"
                #,'AdditionalSlaveSecurityGroups': slave_additional_security_group,
            },
        
            Steps=[],
        
            Applications=[
                {
                    'Name': 'Hadoop'
                },
                {
                    'Name': 'Spark'
                },
            ],
            Configurations=[
                {
                    'Classification': 'Spark',
                    'Properties': {
                        'maximizeResourceAllocation': "true"
                    },
                    "Configurations" : []
                },
            ],
            VisibleToAllUsers=True,
            #JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
    )

    client.add_job_flow_steps(
            JobFlowId=response['JobFlowId'],
            Steps=get_dist_cp_step("1.venkat", "emr/input/all_station_master.csv", "emr/output/all_station_master.csv")
                            )
    
    #client.add_job_flow_steps(
    #        JobFlowId=response['JobFlowId'],
    #        Steps=get_dist_cp_step("1.venkat", "emr/input", "emr/output"),)
    
    return response['JobFlowId']
    
    
clusterName = create_cluster()

def check_cluster_status(region_name, jobflow_id):
    client = boto3.client('emr', region_name=region_name)
    response = client.list_clusters()
    for cluster in response['Clusters']:
        if cluster['Id'] == jobflow_id:
            
            if(cluster['Status']['StateChangeReason']['Message']):
                return (cluster['Status']['State'] , cluster['Status']['StateChangeReason']['Message'])
            else:
                return (cluster['Status']['State'], None)
    return None
check_cluster_status(region_name,clusterName)



def get_dist_cp_step(bucket_name, source_path, target_path):
    return [{
        'Name': 'S3DistCp step',
        'ActionOnFailure': 'CANCEL_AND_WAIT',#'TERMINATE_JOB_FLOW'|'TERMINATE_CLUSTER'|'CANCEL_AND_WAIT'|'CONTINUE',
        'HadoopJarStep': {
            'Jar': '/usr/share/aws/emr/s3-dist-cp/lib/s3-dist-cp.jar',
            'Args': [
                '--src',
                's3://{0}/{1}'.format(bucket_name, source_path),
                '--dest',
                's3://{0}/{1}'.format(bucket_name, target_path),
                '--groupBy',
                "(.)*",
                '--targetSize',
                '2048',
            ]
        }
    }]
#get_dist_cp_step("1.venkat", "emr/input/", "emr/output/")
get_dist_cp_step("1.venkat", "emr/input/all_station_master.csv", "emr/output/all_station_master.csv")
