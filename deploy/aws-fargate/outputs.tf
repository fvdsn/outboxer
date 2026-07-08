output "region" {
  value = var.region
}

output "cluster" {
  value = aws_ecs_cluster.outboxer.name
}

output "service" {
  value = local.name
}

output "task_definition" {
  value = aws_ecs_task_definition.outboxer.family
}

output "queue_url" {
  value = aws_sqs_queue.events.url
}

output "fifo_queue_url" {
  value = aws_sqs_queue.events_fifo.url
}

output "db_host" {
  value = aws_db_instance.outboxer.address
}

output "db_name" {
  value = aws_db_instance.outboxer.db_name
}

output "db_user" {
  value = aws_db_instance.outboxer.username
}

output "db_password" {
  value     = random_password.db.result
  sensitive = true
}

output "dlq_table" {
  value = "outboxer_dead_letters"
}

output "ecr_repository" {
  value = aws_ecr_repository.outboxer.repository_url
}

# Consumed by up.sh for the init run-task network configuration.
output "subnet_ids" {
  value = join(",", aws_subnet.public[*].id)
}

output "task_security_group" {
  value = aws_security_group.task.id
}
