terraform {
  required_version = ">= 1.7.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Service = "nabedra-events", Cluster = var.name }
  }
}

data "aws_partition" "current" {}

data "aws_subnet" "broker" {
  for_each = var.brokers
  id       = each.value.subnet_id
}

data "aws_subnet" "ingress" {
  for_each = var.ingress_subnet_ids
  id       = each.key
}

data "aws_route53_zone" "peers" {
  zone_id = var.peer_zone_id
}

locals {
  instances = { for id, broker in var.brokers : id => broker if broker.provision_instance }
  peer_names = {
    for id, broker in var.brokers : id => "broker-${id}.${trimsuffix(data.aws_route53_zone.peers.name, ".")}"
  }
  bootstrap = {
    metadata_voters = var.metadata_voters
    brokers = [
      for id, broker in var.brokers : {
        id       = tonumber(id)
        zone     = data.aws_subnet.broker[id].availability_zone
        peer_url = "https://${local.peer_names[id]}:8081"
      }
    ]
  }
}

resource "terraform_data" "topology" {
  lifecycle {
    precondition {
      condition = alltrue([
        for subnet in concat(values(data.aws_subnet.broker), values(data.aws_subnet.ingress)) :
        subnet.vpc_id == var.vpc_id
      ])
      error_message = "Broker and ingress subnets must belong to the Events VPC."
    }
    precondition {
      condition     = data.aws_route53_zone.peers.private_zone
      error_message = "Broker peers require a private Route 53 zone associated with the VPC."
    }
    precondition {
      condition = alltrue([for id in var.metadata_voters : contains(keys(var.brokers), tostring(id))]) && length(toset([
        for id in var.metadata_voters : try(data.aws_subnet.broker[tostring(id)].availability_zone, "")
      ])) == 3
      error_message = "The three bootstrap metadata voters must exist in three different availability zones."
    }
    precondition {
      condition = (
        length(data.aws_subnet.ingress) >= 3 &&
        length(toset([for subnet in data.aws_subnet.ingress : subnet.availability_zone])) == length(data.aws_subnet.ingress) &&
        alltrue([
          for id in keys(local.instances) : contains(
            [for subnet in data.aws_subnet.ingress : subnet.availability_zone],
            data.aws_subnet.broker[id].availability_zone
          )
        ])
      )
      error_message = "Specify one ingress subnet in each broker zone and at least three zones overall."
    }
  }
}

resource "aws_security_group" "broker" {
  name_prefix = "${var.name}-broker-"
  description = "Events brokers and private Raft traffic"
  vpc_id      = var.vpc_id
}

resource "aws_security_group" "ingress" {
  name_prefix = "${var.name}-ingress-"
  description = "Events client HTTPS load balancer"
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "https" {
  for_each          = var.client_ipv4_cidrs
  security_group_id = aws_security_group.ingress.id
  cidr_ipv4         = each.key
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_ingress_rule" "broker_api" {
  security_group_id            = aws_security_group.broker.id
  referenced_security_group_id = aws_security_group.ingress.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
}

resource "aws_vpc_security_group_egress_rule" "broker_api" {
  security_group_id            = aws_security_group.ingress.id
  referenced_security_group_id = aws_security_group.broker.id
  ip_protocol                  = "tcp"
  from_port                    = 8080
  to_port                      = 8080
}

resource "aws_vpc_security_group_ingress_rule" "peer" {
  security_group_id            = aws_security_group.broker.id
  referenced_security_group_id = aws_security_group.broker.id
  ip_protocol                  = "tcp"
  from_port                    = 8081
  to_port                      = 8081
}

resource "aws_vpc_security_group_egress_rule" "peer" {
  security_group_id            = aws_security_group.broker.id
  referenced_security_group_id = aws_security_group.broker.id
  ip_protocol                  = "tcp"
  from_port                    = 8081
  to_port                      = 8081
}

resource "aws_vpc_security_group_egress_rule" "aws_services" {
  for_each          = var.service_ipv4_cidrs
  security_group_id = aws_security_group.broker.id
  cidr_ipv4         = each.key
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_ebs_volume" "data" {
  for_each          = var.brokers
  availability_zone = data.aws_subnet.broker[each.key].availability_zone
  size              = each.value.disk_gib
  type              = "gp3"
  iops              = each.value.disk_iops
  throughput        = each.value.disk_throughput_mib
  encrypted         = true
  kms_key_id        = var.disk_kms_key_arn == "" ? null : var.disk_kms_key_arn
  tags              = { Name = "${var.name}-data-${each.key}", BrokerId = each.key }

  lifecycle {
    prevent_destroy = true
  }
  depends_on = [terraform_data.topology]
}

resource "aws_instance" "broker" {
  for_each                    = local.instances
  ami                         = each.value.ami_id
  instance_type               = each.value.instance_type
  subnet_id                   = each.value.subnet_id
  vpc_security_group_ids      = [aws_security_group.broker.id]
  associate_public_ip_address = false
  iam_instance_profile        = aws_iam_instance_profile.broker[each.key].name

  user_data_base64 = base64gzip(templatefile("${path.module}/cloud-init.sh.tftpl", {
    deployment = base64encode(jsonencode({
      region              = var.region
      volume_id           = aws_ebs_volume.data[each.key].id
      format_blank_volume = each.value.format_blank_volume
      secret_arn          = each.value.secret_arn
      artifact_bucket     = var.artifact_bucket
      artifact_key        = each.value.artifact_key
      artifact_sha256     = each.value.artifact_sha256
      config = {
        broker_id       = tonumber(each.key)
        cluster_id      = var.cluster_id
        public_bind     = "0.0.0.0:8080"
        peer_bind       = "0.0.0.0:8081"
        data_dir        = "/var/lib/nabedra-events"
        allow_plaintext = false
        bootstrap       = local.bootstrap
      }
    }))
    prepare_script = base64encode(file("${path.module}/prepare.sh"))
    start_broker   = each.value.start_broker
  }))
  lifecycle {
    ignore_changes = [user_data_base64]
  }

  root_block_device {
    encrypted             = true
    volume_type           = "gp3"
    delete_on_termination = true
  }
  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }
  tags       = { Name = "${var.name}-broker-${each.key}", BrokerId = each.key }
  depends_on = [terraform_data.topology, aws_iam_role_policy.broker, aws_iam_role_policy_attachment.ssm]
}

resource "aws_volume_attachment" "data" {
  for_each                       = local.instances
  device_name                    = "/dev/sdf"
  volume_id                      = aws_ebs_volume.data[each.key].id
  instance_id                    = aws_instance.broker[each.key].id
  stop_instance_before_detaching = true
}

resource "aws_route53_record" "peer" {
  for_each = local.instances
  zone_id  = var.peer_zone_id
  name     = local.peer_names[each.key]
  type     = "A"
  ttl      = 30
  records  = [aws_instance.broker[each.key].private_ip]
}

resource "aws_lb" "client" {
  name                       = "${var.name}-events"
  internal                   = var.internal_ingress
  load_balancer_type         = "application"
  subnets                    = var.ingress_subnet_ids
  security_groups            = [aws_security_group.ingress.id]
  drop_invalid_header_fields = true
  desync_mitigation_mode     = "strictest"
  depends_on                 = [terraform_data.topology]
}

resource "aws_lb_target_group" "broker" {
  name                 = "${var.name}-events"
  port                 = 8080
  protocol             = "HTTPS"
  target_type          = "instance"
  vpc_id               = var.vpc_id
  deregistration_delay = 15
  health_check {
    path     = "/readyz"
    protocol = "HTTPS"
    matcher  = "200"
  }
}

resource "aws_lb_target_group_attachment" "broker" {
  for_each         = local.instances
  target_group_arn = aws_lb_target_group.broker.arn
  target_id        = aws_instance.broker[each.key].id
  depends_on       = [aws_volume_attachment.data]
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.client.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.client_certificate_arn
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.broker.arn
  }
}

resource "aws_route53_record" "client" {
  zone_id = var.client_zone_id
  name    = var.client_dns_name
  type    = "A"
  alias {
    name                   = aws_lb.client.dns_name
    zone_id                = aws_lb.client.zone_id
    evaluate_target_health = true
  }
}

output "events_url" {
  value = "https://${trimsuffix(var.client_dns_name, ".")}"
}

output "peer_urls" {
  value = { for id, name in local.peer_names : id => "https://${name}:8081" }
}

output "volumes" {
  value = { for id, volume in aws_ebs_volume.data : id => volume.id }
}

output "backup_uri" {
  value = "s3://${var.backup_bucket}/${var.backup_prefix}/"
}
