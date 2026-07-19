output "public_ip" {
  description = "Elastic IP of the instance — the dashboard is at http://<this>"
  value       = aws_eip.pageledger.public_ip
}

output "instance_id" {
  description = "EC2 instance id (for start/stop)"
  value       = aws_instance.pageledger.id
}

output "ssh_command" {
  description = "SSH into the instance"
  value       = "ssh ec2-user@${aws_eip.pageledger.public_ip}"
}

output "ssm_command" {
  description = "Shell in from anywhere (no open port / IP needed)"
  value       = "aws ssm start-session --target ${aws_instance.pageledger.id}"
}

output "dashboard_url" {
  value = "http://${aws_eip.pageledger.public_ip}"
}
