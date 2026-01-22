output "s3_bucket_id" {
  description = "The name of the bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  description = "The ARN of the bucket"
  value       = aws_s3_bucket.data_lake.arn
}
