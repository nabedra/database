data "aws_iam_policy_document" "ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "broker" {
  for_each           = local.instances
  name               = "${var.name}-events-broker-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.ec2.json
}

resource "aws_iam_instance_profile" "broker" {
  for_each = local.instances
  name     = aws_iam_role.broker[each.key].name
  role     = aws_iam_role.broker[each.key].name
}

resource "aws_iam_role_policy_attachment" "ssm" {
  for_each   = local.instances
  role       = aws_iam_role.broker[each.key].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "broker" {
  for_each = local.instances
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [each.value.secret_arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.artifact_bucket}/${each.value.artifact_key}"]
  }
  dynamic "statement" {
    for_each = length(var.kms_key_arns) == 0 ? [] : [var.kms_key_arns]
    content {
      actions   = ["kms:Decrypt"]
      resources = statement.value
      condition {
        test     = "StringEquals"
        variable = "kms:ViaService"
        values = [
          "s3.${var.region}.${data.aws_partition.current.dns_suffix}",
          "secretsmanager.${var.region}.${data.aws_partition.current.dns_suffix}",
        ]
      }
    }
  }
}

resource "aws_iam_role_policy" "broker" {
  for_each = local.instances
  name     = "events-config-and-artifact"
  role     = aws_iam_role.broker[each.key].name
  policy   = data.aws_iam_policy_document.broker[each.key].json
}

data "aws_iam_policy_document" "backup_operator" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.backup_bucket}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.backup_prefix}/*"]
    }
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.backup_bucket}/${var.backup_prefix}/*"]
  }
  statement {
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = [var.backup_kms_key_arn]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["s3.${var.region}.${data.aws_partition.current.dns_suffix}"]
    }
  }
}

resource "aws_iam_policy" "backup_operator" {
  name        = "${var.name}-events-backups"
  description = "Read and write named Events backup bundles without deleting objects"
  policy      = data.aws_iam_policy_document.backup_operator.json
}

output "backup_operator_policy_arn" {
  value = aws_iam_policy.backup_operator.arn
}
