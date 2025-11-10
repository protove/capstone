# ========================================
# Lambda Function: EC2 GPU 인스턴스 자동 시작
# ========================================

# Lambda IAM Role
resource "aws_iam_role" "sqs_to_ec2_lambda" {
  name = "capstone-${var.environment}-sqs-to-ec2-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "capstone-sqs-to-ec2-lambda"
    Environment = var.environment
    Project     = "Unmanned"
  }
}

# Lambda Execution Policy
resource "aws_iam_role_policy" "sqs_to_ec2_lambda" {
  name = "capstone-${var.environment}-sqs-to-ec2-lambda-policy"
  role = aws_iam_role.sqs_to_ec2_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Sid    = "SQSAccess"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility"
        ]
        Resource = aws_sqs_queue.video_processing.arn
      },
      {
        Sid    = "EC2InstanceControl"
        Effect = "Allow"
        Action = [
          "ec2:StartInstances",
          "ec2:StopInstances",
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceStatus"
        ]
        Resource = "*"  # 특정 인스턴스로 제한 가능
      }
    ]
  })
}

# Lambda 함수 소스 코드 압축
data "archive_file" "sqs_to_ec2_lambda" {
  type        = "zip"
  output_path = "${path.module}/lambda_deployment.zip"

  source {
    content  = file("${path.module}/../lambda/ec2_starter.py")
    filename = "ec2_starter.py"
  }
}

# Lambda Function
resource "aws_lambda_function" "sqs_to_ec2" {
  filename         = data.archive_file.sqs_to_ec2_lambda.output_path
  function_name    = "capstone-${var.environment}-sqs-to-ec2"
  role             = aws_iam_role.sqs_to_ec2_lambda.arn
  handler          = "ec2_starter.lambda_handler"
  source_code_hash = data.archive_file.sqs_to_ec2_lambda.output_base64sha256
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      GPU_INSTANCE_ID = var.gpu_instance_id
      SQS_QUEUE_URL   = aws_sqs_queue.video_processing.url
      ENVIRONMENT     = var.environment
    }
  }

  tags = {
    Name        = "capstone-sqs-to-ec2"
    Environment = var.environment
    Project     = "Unmanned"
  }

  depends_on = [aws_iam_role_policy.sqs_to_ec2_lambda]
}

# CloudWatch Log Group for Lambda
resource "aws_cloudwatch_log_group" "sqs_to_ec2_lambda" {
  name              = "/aws/lambda/${aws_lambda_function.sqs_to_ec2.function_name}"
  retention_in_days = 7

  tags = {
    Name        = "capstone-sqs-to-ec2-lambda-logs"
    Environment = var.environment
    Project     = "Unmanned"
  }
}

# Lambda Event Source Mapping (SQS Trigger)
resource "aws_lambda_event_source_mapping" "sqs_to_ec2" {
  event_source_arn = aws_sqs_queue.video_processing.arn
  function_name    = aws_lambda_function.sqs_to_ec2.arn
  batch_size       = 1  # 한 번에 1개 메시지 처리
  enabled          = true

  # Partial Batch Response 활성화 (실패한 메시지만 재시도)
  function_response_types = ["ReportBatchItemFailures"]

  # 동시 실행 제한
  scaling_config {
    maximum_concurrency = 2  # GPU 인스턴스는 1개만 필요하므로 제한
  }
}

# ========================================
# Outputs
# ========================================

output "lambda_function_arn" {
  description = "Lambda Function ARN for EC2 GPU starter"
  value       = aws_lambda_function.sqs_to_ec2.arn
}

output "lambda_function_name" {
  description = "Lambda Function Name"
  value       = aws_lambda_function.sqs_to_ec2.function_name
}

output "lambda_log_group" {
  description = "CloudWatch Log Group for Lambda"
  value       = aws_cloudwatch_log_group.sqs_to_ec2_lambda.name
}
