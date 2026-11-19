"""Render version 1 of the private Nabedra Cache CloudFormation template."""

PORT = 6379
CONTROL = 7400
RAFT = 7401
PEER = 7500


def ref(name):
    return {"Ref": name}


def attr(resource, name):
    return {"Fn::GetAtt": [resource, name]}


def sub(text, **values):
    return {"Fn::Sub": [text, values] if values else text}


def resource(kind, **properties):
    return {"Type": kind, "Properties": properties}


def render(options, network, artifact, replacements=None):
    replacements = replacements or {}
    resources = {}
    zones = [f"Zone{index}" for index in range(1, 4)]
    parameters = {
        key: {"Type": "AWS::EC2::AvailabilityZone::Name", "Default": zone}
        for key, zone in zip(zones, network["zones"], strict=True)
    }
    fresh = "vpc" not in network
    if fresh:
        resources["Vpc"] = resource(
            "AWS::EC2::VPC",
            CidrBlock=network["cidr"],
            EnableDnsSupport=True,
            EnableDnsHostnames=True,
        )
        vpc = ref("Vpc")
        subnets = []
        route_tables = []
        for index, (zone, cidr) in enumerate(zip(zones, network["subnetCidrs"], strict=True), 1):
            name, route = f"Subnet{index}", f"RouteTable{index}"
            resources[name] = resource(
                "AWS::EC2::Subnet", VpcId=vpc, AvailabilityZone=ref(zone),
                CidrBlock=cidr, MapPublicIpOnLaunch=False,
            )
            resources[route] = resource("AWS::EC2::RouteTable", VpcId=vpc)
            resources[f"SubnetRoute{index}"] = resource(
                "AWS::EC2::SubnetRouteTableAssociation", SubnetId=ref(name), RouteTableId=ref(route)
            )
            subnets.append(ref(name))
            route_tables.append(ref(route))
    else:
        vpc, subnets = network["vpc"], network["subnets"]

    def security_group(name, description):
        resources[name] = resource("AWS::EC2::SecurityGroup", VpcId=vpc, GroupDescription=description)
        return ref(name)

    def ingress(name, target, port, *, source=None, cidr=None):
        selector = {"SourceSecurityGroupId": source} if source is not None else {"CidrIp": cidr}
        resources[name] = resource(
            "AWS::EC2::SecurityGroupIngress", GroupId=target, IpProtocol="tcp",
            FromPort=port, ToPort=port, **selector,
        )

    controller = security_group("ControllerGroup", "Cache metadata voters and private control")
    worker = security_group("WorkerGroup", "Cache clients and RAM replication")
    ingress("ControllerConsensus", controller, RAFT, source=controller)
    ingress("ManagementControl", controller, CONTROL, source=controller)
    ingress("WorkerControl", controller, CONTROL, source=worker)
    ingress("ManagementClients", worker, PORT, source=controller)
    ingress("WorkerReplication", worker, PEER, source=worker)
    for index, application in enumerate(network["applications"], 1):
        if application.startswith("sg-"):
            ingress(f"Application{index}", worker, PORT, source=application)
        else:
            ingress(f"Application{index}", worker, PORT, cidr=application)

    if fresh:
        endpoints = security_group("EndpointGroup", "Private bootstrap and Systems Manager endpoints")
        ingress("ControllersToEndpoints", endpoints, 443, source=controller)
        ingress("WorkersToEndpoints", endpoints, 443, source=worker)
        for logical, service in (
            ("Ssm", "ssm"), ("SsmMessages", "ssmmessages"), ("SecretsManager", "secretsmanager"),
        ):
            resources[f"VpcEndpoint{logical}"] = resource(
                "AWS::EC2::VPCEndpoint", VpcId=vpc, VpcEndpointType="Interface",
                ServiceName=sub("com.amazonaws.${AWS::Region}." + service),
                SubnetIds=subnets, PrivateDnsEnabled=True, SecurityGroupIds=[endpoints],
            )
        resources["VpcEndpointS3"] = resource(
            "AWS::EC2::VPCEndpoint", VpcId=vpc, VpcEndpointType="Gateway",
            ServiceName=sub("com.amazonaws.${AWS::Region}.s3"), RouteTableIds=route_tables,
        )
    resources["Dns"] = resource(
        "AWS::Route53::HostedZone", Name=options.domain, VPCs=[{"VPCId": vpc, "VPCRegion": ref("AWS::Region")}],
    )
    outputs = {}
    for kind, count in (("controller", 3), ("worker", options.workers)):
        controller_role = kind == "controller"
        group = controller if controller_role else worker
        instance_type = options.controller_type if controller_role else options.worker_type
        for node in range(1, count + 1):
            logical = f"{kind.title()}{node}"
            name = f"{kind[0]}{node}.{options.domain}"
            zone_index = (node - 1) % 3
            secret = f"{options.bootstrap_prefix}/{kind}/{node}"
            secrets = [secret]
            if controller_role and node == 1:
                secrets.append(f"{options.bootstrap_prefix}/operator")
            resources[f"{logical}Role"] = resource(
                "AWS::IAM::Role",
                AssumeRolePolicyDocument={
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Action": "sts:AssumeRole", "Effect": "Allow",
                        "Principal": {"Service": "ec2.amazonaws.com"},
                    }],
                },
                ManagedPolicyArns=[sub("arn:${AWS::Partition}:iam::aws:policy/AmazonSSMManagedInstanceCore")],
                Policies=[{
                    "PolicyName": "CacheBootstrap",
                    "PolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow", "Action": "secretsmanager:GetSecretValue",
                                "Resource": [
                                    sub("arn:${AWS::Partition}:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:" + item + "-??????")
                                    for item in secrets
                                ],
                            },
                            {
                                "Effect": "Allow", "Action": "s3:GetObjectVersion",
                                "Resource": sub("arn:${AWS::Partition}:s3:::" + artifact["bucket"] + "/" + artifact["key"]),
                                "Condition": {"StringEquals": {"s3:VersionId": artifact["version"]}},
                            },
                        ],
                    },
                }],
            )
            resources[f"{logical}Profile"] = resource(
                "AWS::IAM::InstanceProfile", Roles=[ref(f"{logical}Role")]
            )
            if controller_role:
                resources[f"{logical}Metadata"] = {
                    **resource(
                        "AWS::EC2::Volume", AvailabilityZone=ref(zones[zone_index]), Encrypted=True,
                        VolumeType="gp3", Size=8, Tags=[{"Key": "Name", "Value": f"{name}-metadata"}],
                    ),
                    "DeletionPolicy": "Retain",
                    "UpdateReplacePolicy": "Retain",
                }
            config = [
                options.region, kind, str(node), secret, options.bootstrap_prefix,
                artifact["bucket"], artifact["key"], artifact["version"], artifact["sha256"],
                "${Metadata}" if controller_role else "-",
                str(options.controller_memory_mib if controller_role else options.worker_memory_mib),
            ]
            user_data = """#!/bin/bash
set -euo pipefail
umask 077
bundle=$(mktemp /run/nabedra-bootstrap.XXXXXX)
trap 'rm -f "$bundle"' EXIT
for attempt in $(seq 1 18); do
    aws --region '{}' s3api get-object --bucket '{}' --key '{}' --version-id '{}' "$bundle" >/dev/null && break
    sleep 5
done
printf '%s  %s\n' '{}' "$bundle" | sha256sum --check --status
tar -xOzf "$bundle" bootstrap.sh | /bin/bash -s -- {} "$bundle"
""".format(options.region, artifact["bucket"], artifact["key"], artifact["version"], artifact["sha256"],
           " ".join(f"'{part}'" for part in config))
            if controller_role:
                user_data = sub(user_data, Metadata=ref(f"{logical}Metadata"))
            launch = f"{logical}Launch"
            resources[launch] = resource(
                "AWS::EC2::LaunchTemplate",
                VersionDescription="nabedra-cache-cloudformation-v1",
                LaunchTemplateData={
                    "ImageId": options.ami,
                    "InstanceType": instance_type,
                    "IamInstanceProfile": {"Arn": attr(f"{logical}Profile", "Arn")},
                    "MetadataOptions": {"HttpEndpoint": "enabled", "HttpTokens": "required"},
                    "NetworkInterfaces": [{
                        "AssociatePublicIpAddress": False, "DeviceIndex": 0, "Groups": [group],
                        "SubnetId": subnets[zone_index], "DeleteOnTermination": True,
                    }],
                    "BlockDeviceMappings": [{
                        "DeviceName": options.root_device,
                        "Ebs": {"Encrypted": True, "VolumeType": "gp3", "VolumeSize": 16, "DeleteOnTermination": True},
                    }],
                    "UserData": {"Fn::Base64": user_data},
                    "TagSpecifications": [{
                        "ResourceType": "instance",
                        "Tags": [{"Key": "Name", "Value": name},
                                 {"Key": "NabedraCacheStack", "Value": options.stack},
                                 {"Key": "NabedraCacheRole", "Value": kind},
                                 {"Key": "NabedraCacheNode", "Value": str(node)},
                                 {"Key": "NabedraCacheReplacement",
                                  "Value": str(replacements.get(str(node), 0) if not controller_role else 0)}],
                    }],
                },
            )
            resources[logical] = resource(
                "AWS::EC2::Instance", LaunchTemplate={
                    "LaunchTemplateId": ref(launch), "Version": attr(launch, "LatestVersionNumber"),
                },
            )
            if fresh:
                resources[logical]["DependsOn"] = [
                    "VpcEndpointSsm", "VpcEndpointSsmMessages", "VpcEndpointSecretsManager", "VpcEndpointS3",
                ]
            if controller_role:
                resources[f"{logical}Attachment"] = resource(
                    "AWS::EC2::VolumeAttachment", Device="/dev/sdf",
                    InstanceId=ref(logical), VolumeId=ref(f"{logical}Metadata"),
                )
            resources[f"{logical}Dns"] = resource(
                "AWS::Route53::RecordSet", HostedZoneId=ref("Dns"), Name=name, Type="A",
                TTL="10", ResourceRecords=[attr(logical, "PrivateIp")],
            )
            outputs[logical] = {"Value": ref(logical)}
    outputs["PrivateSeeds"] = {
        "Value": ",".join(f"rediss://w{node}.{options.domain}:{PORT}" for node in range(1, 4)),
    }
    outputs["VpcId"] = {"Value": vpc}
    outputs["Workers"] = {"Value": str(options.workers)}
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Nabedra Cache private single-region cluster (template v1)",
        "Metadata": {"NabedraCache": {"TemplateVersion": 1, "Workers": options.workers,
                                      "BootstrapPrefix": options.bootstrap_prefix}},
        "Parameters": parameters,
        "Resources": resources,
        "Outputs": outputs,
    }
