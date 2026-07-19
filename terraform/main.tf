# Single EC2 instance running the whole Docker Compose stack (no managed AWS data services).
terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    http = {
      source  = "hashicorp/http"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Auto-detect the current public IP so the SSH rule matches wherever apply runs (re-apply after an IP change).
data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  ssh_cidr = var.allowed_ssh_cidr != "" ? var.allowed_ssh_cidr : "${chomp(data.http.my_ip.response_body)}/32"
}

# Always-latest Amazon Linux 2023 x86_64 AMI (via the public SSM parameter).
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_key_pair" "deployer" {
  key_name   = "pageledger-key"
  public_key = file(var.ssh_public_key_path)

  tags = { Project = "pageledger" }
}

# SSM Session Manager role: shell in from anywhere via AWS creds, the lockout-proof fallback if your IP changes.
data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pageledger" {
  name               = "pageledger-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
  tags               = { Project = "pageledger" }
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.pageledger.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "pageledger" {
  name = "pageledger-instance-profile"
  role = aws_iam_role.pageledger.name
}

resource "aws_security_group" "pageledger" {
  name        = "pageledger-sg"
  description = "PageLedger: HTTP 80 from anywhere, SSH 22 from operator only, all egress"

  ingress {
    description = "HTTP dashboard (public)"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "SSH (operator IP only, auto-detected; SSM is the lockout-proof fallback)"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.ssh_cidr]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Project = "pageledger" }
}

resource "aws_instance" "pageledger" {
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = var.instance_type
  key_name               = aws_key_pair.deployer.key_name
  vpc_security_group_ids = [aws_security_group.pageledger.id]
  iam_instance_profile   = aws_iam_instance_profile.pageledger.name
  user_data              = file("${path.module}/user-data.sh")

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.volume_size
    delete_on_termination = true
  }

  tags = {
    Name    = "pageledger"
    Project = "pageledger"
  }
}

# Elastic IP: stable address across stop/start (a destroy releases it; the next apply gets a new one).
resource "aws_eip" "pageledger" {
  instance = aws_instance.pageledger.id
  domain   = "vpc"

  tags = { Project = "pageledger" }
}
