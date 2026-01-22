resource "aws_iam_user" "data_loader" {
  name = "sleeplens_data_loader"
  path = "/system/"

  tags = {
    Environment = var.environment
  }
}

resource "aws_iam_access_key" "data_loader" {
  user = aws_iam_user.data_loader.name
}

resource "aws_iam_user_policy" "data_loader_s3" {
  name = "sleeplens_data_loader_s3_access"
  user = aws_iam_user.data_loader.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
    ]
  })
}

output "data_loader_access_key_id" {
  value = aws_iam_access_key.data_loader.id
}

output "data_loader_secret_access_key" {
  value     = aws_iam_access_key.data_loader.secret
  sensitive = true
}
